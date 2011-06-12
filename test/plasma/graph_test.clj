(ns plasma.graph-test
  (:use [plasma util graph]
        [jiraph graph]
        test-utils
        clojure.test
        clojure.stacktrace)
  (:require [logjam.core :as log]
            [lamina.core :as lamina]))

(deftest add-remove-test
  (let [ids ["a" "b" "c" "d"]
        deleter (fn [] (doall (map #(delete-node! :graph %) ids)))
        tgt (first ids)
        srcs (drop 1 ids)]
    (try
      (doseq [id ids]
        (make-node id))
      (doseq [src srcs]
        (make-edge src tgt {:label :foo}))
      (is (= 3 (count (incoming-nodes tgt))))
      (remove-edge "b" tgt)
      (is (= 2 (count (incoming-nodes tgt))))
      (remove-edge "c" tgt #(= :bar (:label %)))
      (is (= 2 (count (incoming-nodes tgt))))
      (remove-edge "c" tgt #(= :foo (:label %)))
      (is (= 1 (count (incoming-nodes tgt))))
      (remove-node tgt)
      (is (nil? (find-node tgt)))
      (is (= 0 (count (incoming-nodes tgt))))
      (finally
        (deleter)))))

(deftest node-event-test
  (let [n-id (make-node)
        event-chan (node-event-channel n-id)]
    (#'plasma.graph/node-event n-id {:a 10 :b 20})
    (assoc-node n-id :label "test" :owner "all")
    (dotimes [i 100]
      (assoc-node n-id :val i))
    (Thread/sleep 10)
    (let [[a b & c] (take 102 (lamina/channel-seq event-chan 100))]
      (is (= 20 (get-in a [:props :b])))
      (is (= {:label "test" :owner "all"}
             (select-keys (:props b) [:label :owner])))
      (is (= (range 100) (map (comp :val :props) c))))))

(deftest edge-event-test
  (let [n-id (make-node)
        c1 (edge-event-channel n-id)
        c2 (edge-event-channel n-id :foo)
        c3 (edge-event-channel n-id #(>= (:weight %) 50))]
    (dotimes [i 100]
      (make-edge n-id (make-node) {:label :foo :weight i}))
    (Thread/sleep 10)
    (let [r1 (lamina/channel-seq c1 10)
          r2 (lamina/channel-seq c2 10)
          r3 (lamina/channel-seq c3 10)]
      ;(println "r3: " r3)
      (is (= 100 (count r1) (count r2)))
      (is (= 50 (count r3)))
      (is (= (range 100)
             (map (comp :weight :props) r1)
             (map (comp :weight :props) r2)))
      (is (= (range 50 100 1)
             (map (comp :weight :props) r3))))))


(use-fixtures :once test-fixture)
