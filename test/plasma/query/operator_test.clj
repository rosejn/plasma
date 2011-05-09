(ns plasma.query.operator-test
  (:use [plasma util graph]
        [plasma.query operator]
        [clojure test stacktrace]
        [lamina core]
        [jiraph graph]
        test-utils)
  (:require [logjam.core :as log]))

(defn parameter-op-test []
  (let [id (uuid)
        p1 (parameter-op id)
        p2 (parameter-op id)]
    (enqueue-and-close (:in p1) 42)
    (enqueue-and-close (:in p2) ROOT-ID)
    (is (= {id 42}
           (first (lazy-channel-seq (:out p1)))))
    (is (= {id ROOT-ID}
           (first (lazy-channel-seq (:out p2)))))
    (is (and
          (closed? (:out p1))
          (closed? (:out p2))))))

(defn traverse-op-test []
  (let [id (uuid)
        p1 (parameter-op id ROOT-ID)
        t1 (traverse-op (uuid) nil nil (:id p1) :music)
        j1 (join-op (uuid) p1 t1)]
    (enqueue-and-close (:in p1) ROOT-ID)
    (is (uuid? (get (first (channel-seq (:out j1) 500)) (:id t1))))
    (is (and
          (closed? (:out p1))
          (closed? (:out t1))
          (closed? (:out j1))))))

; (path [synth [:music :synths :synth]]
;   synth)
;   TODO: Don't pass nil to traverse-op, instead we need a way to pass the
;   query envelope so remote-sub queries can be hooked up to running operator
;   tree.
(defn traverse-base []
  (let [p1 (parameter-op (uuid))
        t1 (traverse-op (uuid) nil nil (:id p1) :music)
        j1 (join-op (uuid) p1 t1)
        t2 (traverse-op (uuid) nil nil (:id t1) :synths)
        j2 (join-op (uuid) j1 t2)
        t3 (traverse-op (uuid) nil nil (:id t2) :synth)
        j3 (join-op (uuid) j2 t3)
        r1 (receive-op (uuid) j3 (channel) 200)
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
  (channel-seq (get-in q [:proj :out]) 1000))


; (path [synth [:music :synths :synth]]
;   (where (> (:score synth) 0.3)))
(defn select-op-test []
  (let [tree (traverse-base)
        {:keys [p1 j3 t3]} tree
        sel-pred {:type :plasma.operator/predicate
                  :property :score
                  :operator '>
                  :value 0.3}
        prop-load (property-op (uuid) j3 (:id t3) [(:property sel-pred) :label])
        sel  (select-op (uuid) prop-load (:id t3) sel-pred)
        proj (project-op (uuid) sel [[(:id t3) :label]])
        tree (assoc tree
              :sel sel
              :proj proj)]
    (enqueue-and-close (:in p1) ROOT-ID)
    (Thread/sleep 20)
    (is (= #{:kick :bass :snare}
           (set (map :label (result tree)))))))

(defn aggregate-op-test []
  (let [tree (traverse-base)
        {:keys [p1 r1 t3]} tree
        agg  (aggregate-op (uuid) r1)
        proj (project-op (uuid) agg [[(:id t3)]])
        tree (assoc tree
              :agg agg
              :proj proj)]
    (enqueue-and-close (:in p1) ROOT-ID)
    (is (= #{:kick :bass :snare :hat}
           (set (map (comp :label find-node :id) (result tree)))))))

; (-> (path [synth [:music :synths :synth]])
;   (where (= :kick (:label synth)))
;   (sort synth :score :asc))
(defn sort-op-test []
  (let [tree (traverse-base)
        {:keys [p1 j3 t3]} tree
        pl (property-op (uuid) j3 (:id t3) [:score :label])
        s-desc (sort-op (uuid) pl (:id t3) :score :desc)
        proj-desc (project-op (uuid) s-desc [[(:id t3) :label]])
        tree-desc (assoc tree :proj proj-desc)
        s-asc (sort-op (uuid) pl (:id t3) :score :asc)
        proj-asc (project-op (uuid) s-asc [[(:id t3) :label]])
        tree-asc (assoc tree :proj proj-asc)]
    (enqueue-and-close (:in p1) ROOT-ID)
    (Thread/sleep 20)
    (is (= '(:kick :bass :snare :hat)
           (map :label (result tree-desc))))
    (is (= (reverse '(:kick :bass :snare :hat))
           (map :label (result tree-asc))))))

(defn min-op-test []
  (let [tree (traverse-base)
        {:keys [p1 j3 t3]} tree
        min-desc (min-op (uuid) j3 (:id t3) :score)
        prop-op (property-op (uuid) min-desc (:id t3) [:label])
        proj-desc (project-op (uuid) prop-op [[(:id t3) :label]])
        tree-desc (assoc tree :proj proj-desc)]
    (enqueue-and-close (:in p1) ROOT-ID)
    (Thread/sleep 20)
    (is (= '(:hat)
           (map :label (result tree-desc))))))

(defn max-op-test []
  (let [tree (traverse-base)
        {:keys [p1 j3 t3]} tree
        max-desc (max-op (uuid) j3 (:id t3) :score)
        prop-op (property-op (uuid) max-desc (:id t3) [:label])
        proj-desc (project-op (uuid) prop-op [[(:id t3) :label]])
        tree-desc (assoc tree :proj proj-desc)]
    (enqueue-and-close (:in p1) ROOT-ID)
    (Thread/sleep 20)
    (is (= '(:kick)
           (map :label (result tree-desc))))))

(defn average-op-test []
  (let [tree (traverse-base)
        {:keys [p1 j3 t3]} tree
        avg-desc (average-op (uuid) j3 (:id t3) :score)
        tree-desc (assoc tree :proj avg-desc)]
    (enqueue-and-close (:in p1) ROOT-ID)
    (Thread/sleep 20)
    (is (= (average [0.8 0.3 0.4 0.6]) (first (result tree-desc))))))

(defn limit-op-test []
  (let [tree (traverse-base)
        {:keys [p1 j3 t3]} tree
        limit-desc (limit-op (uuid) j3 2)
        proj-desc (project-op (uuid) limit-desc [[(:id t3)]])
        tree-desc (assoc tree :proj proj-desc)]
    (enqueue-and-close (:in p1) ROOT-ID)
    (Thread/sleep 20)
    (is (= 2 (count (map #(:label (find-node %)) (result tree-desc)))))))

; How do you correctly test something that is supposed to return
; random results?  It seems to be working correctly, but ideally we'd be
; able to check for something a bit more rigorous.
(defn choose-op-test []
  (let [tree (traverse-base)
        {:keys [p1 j3 t3]} tree
        choose-desc (choose-op (uuid) j3 2)
        proj-desc (project-op (uuid) choose-desc [[(:id t3) :label]])
        tree-desc (assoc tree :proj proj-desc)]
    (enqueue-and-close (:in p1) ROOT-ID)
    (Thread/sleep 20)
    (let [res (map :label (result tree-desc))]
      (is (= 2 (count res))))))


(comment defn sub-query-test []
  (let [tree (traverse-base)
        {:keys [t2 j3]} tree]
    (operator-deps j3 (:id t2))))

(defn send-receive-op-test []
  (let [tree (traverse-base)
        {:keys [p1 j3 t3]} tree
        sel-pred {:type :plasma.operator/predicate
                  :property :score
                  :operator '>
                  :value 0.3}
        prop-load (property-op (uuid) j3 (:id t3) [(:property sel-pred) :label])
        sel (select-op (uuid) prop-load (:id t3) sel-pred)
        proj (project-op (uuid) sel [[(:id t3) :label]])
        tree (assoc tree
                    :sel sel
                    :proj proj)]
    (enqueue-and-close (:in p1) ROOT-ID)
    (Thread/sleep 20)
    (is (= #{:kick :bass :snare}
           (set (map :label (result tree)))))))

(comment defn avg-op-test []
  (let [tree (traverse-base)
        {:keys [p1 j3 t3]} tree
        avg-desc (avg-op (uuid) j3 (:id t3) :score)
        proj-desc (project-op (uuid) avg-desc [[(:id t3)]])
        tree-desc (assoc tree :proj proj-desc)]
    (enqueue (:in p1) ROOT-ID)
    (Thread/sleep 20)
    (is (= '(:kick)
           (map #(:label (find-node %)) (result tree-desc))))))

(defn ops-test []
  (parameter-op-test)
  (traverse-op-test)
  (select-op-test)
  (aggregate-op-test)
  (sort-op-test)
  (min-op-test)
  (max-op-test)
  (limit-op-test)
  (choose-op-test)
  (send-receive-op-test))

(deftest test-all
  (test-fixture ops-test))

(defn test-ns-hook
  []
  (test-fixture ops-test))

