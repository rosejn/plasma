(ns plasma.operator-test
  (:use [plasma core operator]
        [clojure test stacktrace]
        [lamina core]
        [jiraph graph]))

(def G
  {:graph (layer "test/db")})

(defn test-graph []
  (node "ROOT" :label :self)
  (with-nodes! [music :music
                synths :synths
                kick  {:label :kick  :score 0.8}
                hat   {:label :hat   :score 0.3}
                snare {:label :snare :score 0.4}
                bass  {:label :bass  :score 0.6}]
               (edge "ROOT" (node :label :net) :label :net)
               (edge "ROOT" music :label :music)
               (edge music synths :label :synths)
               (edge synths bass :label :synth)
               (edge synths hat  :label :synth)
               (edge synths kick :label :synth)
               (edge synths snare :label :synth)))

; (path [synth [:music :synths :synth]]
;   synth)
(defn traverse-base []
  (let [p1 (param-op (uuid))
        r1 (receive-op (uuid))
        t1 (traverse-op (uuid) r1 (:id p1) :music)
        j1 (join-op (uuid) p1 t1)
        t2 (traverse-op (uuid) r1 (:id t1) :synths)
        j2 (join-op (uuid) j1 t2)
        t3 (traverse-op (uuid) r1 (:id t2) :synth)
        j3 (join-op (uuid) j2 t3)
        tree {:p1 p1
              :t1 t1
              :j1 j1
              :t2 t2
              :j2 j2
              :t3 t3
              :j3 j3}]
    tree))

(defn result [q]
  (channel-seq (get-in q [:proj :out])))

(deftest param-op-test []
  (let [p-op (param-op (uuid))
        id (:id p-op)]
    (enqueue (:in p-op) 42)
    (let [[a b] (take 2 (lazy-channel-seq (:out p-op)))]
      (is (= '(42 nil)
             (list (get a id) b))))))

(deftest aggregate-op-test []
  (let [tree (traverse-base)
        {:keys [p1 j3 t3]} tree
        agg  (aggregate-op (uuid) j3)
        proj (project-op (uuid) agg (:id t3))
        tree (assoc tree
              :agg agg
              :proj proj)]
    (enqueue (:in p1) "ROOT")
    (is (= #{:kick :bass :snare :hat}
           (set (map #(:label (find-node %)) (result tree)))))))

; (-> (path [synth [:music :synths :synth]])
;   (where (= :kick (:label synth)))
;   (sort synth :score :asc))
(deftest sort-op-test []
  (let [tree (traverse-base)
        {:keys [p1 j3 t3]} tree
        s-desc (sort-op (uuid) j3 (:id t3) :score :desc)
        proj-desc (project-op (uuid) s-desc (:id t3))
        tree-desc (assoc tree :proj proj-desc)
        s-asc (sort-op (uuid) j3 (:id t3) :score :asc)
        proj-asc (project-op (uuid) s-asc (:id t3))
        tree-asc (assoc tree :proj proj-asc)]
    (enqueue (:in p1) "ROOT")
    (Thread/sleep 20)
    (is (= '(:kick :bass :snare :hat)
           (map #(:label (find-node %)) (result tree-desc))))
    (is (= (reverse '(:kick :bass :snare :hat))
           (map #(:label (find-node %)) (result tree-asc))))))

(deftest min-op-test []
  (let [tree (traverse-base)
        {:keys [p1 j3 t3]} tree
        min-desc (min-op (uuid) j3 (:id t3) :score)
        proj-desc (project-op (uuid) min-desc (:id t3))
        tree-desc (assoc tree :proj proj-desc)]
    (enqueue (:in p1) "ROOT")
    (Thread/sleep 20)
    (is (= '(:hat)
           (map #(:label (find-node %)) (result tree-desc))))))

(deftest max-op-test []
  (let [tree (traverse-base)
        {:keys [p1 j3 t3]} tree
        max-desc (max-op (uuid) j3 (:id t3) :score)
        proj-desc (project-op (uuid) max-desc (:id t3))
        tree-desc (assoc tree :proj proj-desc)]
    (enqueue (:in p1) "ROOT")
    (Thread/sleep 20)
    (is (= '(:kick)
           (map #(:label (find-node %)) (result tree-desc))))))

(deftest limit-op-test []
  (let [tree (traverse-base)
        {:keys [p1 j3 t3]} tree
        limit-desc (limit-op (uuid) j3 2)
        proj-desc (project-op (uuid) limit-desc (:id t3))
        tree-desc (assoc tree :proj proj-desc)]
    (enqueue (:in p1) "ROOT")
    (Thread/sleep 20)
    (is (= 2 (count (map #(:label (find-node %)) (result tree-desc)))))))

; How do you correctly test something that is supposed to return
; random results?  It seems to be working correctly, but ideally we'd be
; able to check for something a bit more rigorous.
(deftest choose-op-test []
  (let [tree (traverse-base)
        {:keys [p1 j3 t3]} tree
        choose-desc (choose-op (uuid) j3 2)
        proj-desc (project-op (uuid) choose-desc (:id t3))
        tree-desc (assoc tree :proj proj-desc)]
    (enqueue (:in p1) "ROOT")
    (Thread/sleep 20)
    (let [res (map #(:label (find-node %)) (result tree-desc))]
      ;(println "res: " res)
      (is (= 2 (count res))))))


; (path [synth [:music :synths :synth]]
;   (where (> (:score synth) 0.3)))
(deftest traverse-test []
  (let [tree (traverse-base)
        {:keys [p1 j3 t3]} tree
        sel-pred {:type :plasma.operator/predicate
                  :property :score
                  :operator '>
                  :value 0.3}
        sel (select-op (uuid) j3 (:id t3) sel-pred)
        proj (project-op (uuid) sel (:id t3))
        tree (assoc tree
              :sel sel
              :proj proj)]
    (enqueue (:in p1) "ROOT")
    (Thread/sleep 20)
    (is (= #{:kick :bass :snare}
           (set (map #(:label (find-node %)) (result tree)))))))

(defn sub-query-test []
  (let [tree (traverse-base)
        {:keys [t2 j3]} tree]
    (operator-deps j3 (:id t2))))

(defn test-fixture [f]
  (with-graph G
    (clear-graph)
    (test-graph)
    (f)))

(use-fixtures :once test-fixture)

(comment deftest avg-op-test []
  (let [tree (traverse-base)
        {:keys [p1 j3 t3]} tree
        avg-desc (avg-op (uuid) j3 (:id t3) :score)
        proj-desc (project-op (uuid) avg-desc (:id t3))
        tree-desc (assoc tree :proj proj-desc)]
    (enqueue (:in p1) "ROOT")
    (Thread/sleep 20)
    (is (= '(:kick)
           (map #(:label (find-node %)) (result tree-desc))))))
