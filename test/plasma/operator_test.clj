(ns plasma.operator-test
  (:use [plasma core operator]
        [clojure test stacktrace]
        [lamina core]
        [jiraph graph]
        test-utils))

; (path [synth [:music :synths :synth]]
;   synth)
;   TODO: Don't pass nil to traverse-op, instead we need a way to pass the
;   query envelope so remote-sub queries can be hooked up to running operator
;   tree.
(defn traverse-base []
  (let [p1 (parameter-op (uuid))
        t1 (traverse-op (uuid) nil (:id p1) :music)
        j1 (join-op (uuid) p1 t1)
        t2 (traverse-op (uuid) nil (:id t1) :synths)
        j2 (join-op (uuid) j1 t2)
        t3 (traverse-op (uuid) nil (:id t2) :synth)
        j3 (join-op (uuid) j2 t3)
        r1 (receive-op (uuid) j3)
        tree {:p1 p1
              :t1 t1
              :j1 j1
              :t2 t2
              :j2 j2
              :t3 t3
              :j3 j3
              :r1 r1}]
    tree))

(defn result [q]
  (channel-seq (get-in q [:proj :out])))

(deftest parameter-op-test []
  (let [p-op (parameter-op (uuid))
        id (:id p-op)]
    (enqueue (:in p-op) 42)
    (let [[a b] (take 2 (lazy-channel-seq (:out p-op)))]
      (is (= '(42 nil)
             (list (get a id) b))))))

(deftest aggregate-op-test []
  (let [tree (traverse-base)
        {:keys [p1 r1 t3]} tree
        agg  (aggregate-op (uuid) r1)
        proj (project-op (uuid) agg (:id t3))
        tree (assoc tree
              :agg agg
              :proj proj)]
    (enqueue (:in p1) ROOT-ID)
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
    (enqueue (:in p1) ROOT-ID)
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
    (enqueue (:in p1) ROOT-ID)
    (Thread/sleep 20)
    (is (= '(:hat)
           (map #(:label (find-node %)) (result tree-desc))))))

(deftest max-op-test []
  (let [tree (traverse-base)
        {:keys [p1 j3 t3]} tree
        max-desc (max-op (uuid) j3 (:id t3) :score)
        proj-desc (project-op (uuid) max-desc (:id t3))
        tree-desc (assoc tree :proj proj-desc)]
    (enqueue (:in p1) ROOT-ID)
    (Thread/sleep 20)
    (is (= '(:kick)
           (map #(:label (find-node %)) (result tree-desc))))))

(deftest limit-op-test []
  (let [tree (traverse-base)
        {:keys [p1 j3 t3]} tree
        limit-desc (limit-op (uuid) j3 2)
        proj-desc (project-op (uuid) limit-desc (:id t3))
        tree-desc (assoc tree :proj proj-desc)]
    (enqueue (:in p1) ROOT-ID)
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
    (enqueue (:in p1) ROOT-ID)
    (Thread/sleep 20)
    (let [res (map #(:label (find-node %)) (result tree-desc))]
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
    (enqueue (:in p1) ROOT-ID)
    (Thread/sleep 20)
    (is (= #{:kick :bass :snare}
           (set (map #(:label (find-node %)) (result tree)))))))

(comment defn sub-query-test []
  (let [tree (traverse-base)
        {:keys [t2 j3]} tree]
    (operator-deps j3 (:id t2))))

(use-fixtures :once test-fixture)

(comment deftest avg-op-test []
  (let [tree (traverse-base)
        {:keys [p1 j3 t3]} tree
        avg-desc (avg-op (uuid) j3 (:id t3) :score)
        proj-desc (project-op (uuid) avg-desc (:id t3))
        tree-desc (assoc tree :proj proj-desc)]
    (enqueue (:in p1) ROOT-ID)
    (Thread/sleep 20)
    (is (= '(:kick)
           (map #(:label (find-node %)) (result tree-desc))))))
