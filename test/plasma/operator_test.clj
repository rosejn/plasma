(ns plasma.operator-test
  (:use [plasma core operator]
        [clojure test stacktrace]
        [lamina core]
        [jiraph graph]
        test-utils)
  (:require [logjam.core :as log]))

(deftest parameter-op-test []
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

(deftest traverse-op-test
  (let [id (uuid)
        p1 (parameter-op id ROOT-ID)
        t1 (traverse-op (uuid) nil (:id p1) :music)
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
  (channel-seq (get-in q [:proj :out]) 1000))

; (path [synth [:music :synths :synth]]
;   (where (> (:score synth) 0.3)))
(deftest select-op-test []
  (let [tree (traverse-base)
        {:keys [p1 j3 t3]} tree
        sel-pred {:type :plasma.operator/predicate
                  :property :score
                  :operator '>
                  :value 0.3}
        prop-load (property-op (uuid) j3 (:id t3) [(:property sel-pred)])
        sel (select-op (uuid) prop-load (:id t3) sel-pred)
        proj (project-op (uuid) sel (:id t3) false)
        tree (assoc tree
              :sel sel
              :proj proj)]
    (enqueue-and-close (:in p1) ROOT-ID)
    (Thread/sleep 20)
    (is (= #{:kick :bass :snare}
           (set (map #(:label (find-node %)) (result tree)))))))

(deftest aggregate-op-test []
  (let [tree (traverse-base)
        {:keys [p1 r1 t3]} tree
        agg  (aggregate-op (uuid) r1)
        proj (project-op (uuid) agg (:id t3) false)
        tree (assoc tree
              :agg agg
              :proj proj)]
    (enqueue-and-close (:in p1) ROOT-ID)
    (is (= #{:kick :bass :snare :hat}
           (set (map #(:label (find-node %)) (result tree)))))))

; (-> (path [synth [:music :synths :synth]])
;   (where (= :kick (:label synth)))
;   (sort synth :score :asc))
(deftest sort-op-test []
  (let [tree (traverse-base)
        {:keys [p1 j3 t3]} tree
        pl (property-op (uuid) j3 (:id t3) [:score])
        s-desc (sort-op (uuid) pl (:id t3) :score :desc)
        proj-desc (project-op (uuid) s-desc (:id t3) false)
        tree-desc (assoc tree :proj proj-desc)
        s-asc (sort-op (uuid) pl (:id t3) :score :asc)
        proj-asc (project-op (uuid) s-asc (:id t3) false)
        tree-asc (assoc tree :proj proj-asc)]
    (enqueue-and-close (:in p1) ROOT-ID)
    (Thread/sleep 20)
    (is (= '(:kick :bass :snare :hat)
           (map #(:label (find-node %)) (result tree-desc))))
    (is (= (reverse '(:kick :bass :snare :hat))
           (map #(:label (find-node %)) (result tree-asc))))))

(deftest min-op-test []
  (let [tree (traverse-base)
        {:keys [p1 j3 t3]} tree
        min-desc (min-op (uuid) j3 (:id t3) :score)
        proj-desc (project-op (uuid) min-desc (:id t3) false)
        tree-desc (assoc tree :proj proj-desc)]
    (enqueue-and-close (:in p1) ROOT-ID)
    (Thread/sleep 20)
    (is (= '(:hat)
           (map #(:label (find-node %)) (result tree-desc))))))

(deftest max-op-test []
  (let [tree (traverse-base)
        {:keys [p1 j3 t3]} tree
        max-desc (max-op (uuid) j3 (:id t3) :score)
        proj-desc (project-op (uuid) max-desc (:id t3) false)
        tree-desc (assoc tree :proj proj-desc)]
    (enqueue-and-close (:in p1) ROOT-ID)
    (Thread/sleep 20)
    (is (= '(:kick)
           (map #(:label (find-node %)) (result tree-desc))))))

(deftest limit-op-test []
  (let [tree (traverse-base)
        {:keys [p1 j3 t3]} tree
        limit-desc (limit-op (uuid) j3 2)
        proj-desc (project-op (uuid) limit-desc (:id t3) false)
        tree-desc (assoc tree :proj proj-desc)]
    (enqueue-and-close (:in p1) ROOT-ID)
    (Thread/sleep 20)
    (is (= 2 (count (map #(:label (find-node %)) (result tree-desc)))))))

; How do you correctly test something that is supposed to return
; random results?  It seems to be working correctly, but ideally we'd be
; able to check for something a bit more rigorous.
(deftest choose-op-test []
  (let [tree (traverse-base)
        {:keys [p1 j3 t3]} tree
        choose-desc (choose-op (uuid) j3 2)
        proj-desc (project-op (uuid) choose-desc (:id t3) false)
        tree-desc (assoc tree :proj proj-desc)]
    (enqueue-and-close (:in p1) ROOT-ID)
    (Thread/sleep 20)
    (let [res (map #(:label (find-node %)) (result tree-desc))]
      (is (= 2 (count res))))))


(comment defn sub-query-test []
  (let [tree (traverse-base)
        {:keys [t2 j3]} tree]
    (operator-deps j3 (:id t2))))

(deftest send-receive-op-test []
  (let [tree (traverse-base)
        {:keys [p1 j3 t3]} tree
        sel-pred {:type :plasma.operator/predicate
                  :property :score
                  :operator '>
                  :value 0.3}
        prop-load (property-op (uuid) j3 (:id t3) [(:property sel-pred)])
        sel (select-op (uuid) prop-load (:id t3) sel-pred)
        proj (project-op (uuid) sel (:id t3) false)
        tree (assoc tree
                    :sel sel
                    :proj proj)]
    (enqueue-and-close (:in p1) ROOT-ID)
    (Thread/sleep 20)
    (is (= #{:kick :bass :snare}
           (set (map #(:label (find-node %)) (result tree)))))))

(comment deftest avg-op-test []
  (let [tree (traverse-base)
        {:keys [p1 j3 t3]} tree
        avg-desc (avg-op (uuid) j3 (:id t3) :score)
        proj-desc (project-op (uuid) avg-desc (:id t3) false)
        tree-desc (assoc tree :proj proj-desc)]
    (enqueue (:in p1) ROOT-ID)
    (Thread/sleep 20)
    (is (= '(:kick)
           (map #(:label (find-node %)) (result tree-desc))))))

(deftest ops-test
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

(defn test-ns-hook 
  []
  (test-fixture ops-test))

