(ns plasma.graph-test
  (:use [plasma config util graph]
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

(deftest event-test
  (let [dest-chan (lamina/channel)]
    (lamina/siphon (node-event-channel "foo") dest-chan)
    (#'plasma.graph/node-event "foo" {:a 1 :b 2} {:a 10 :b 20})
    (Thread/sleep 10)
    (println dest-chan)
    (is (= 20 (get-in (take 1 (lamina/lazy-channel-seq dest-chan 100))
                      [:new-props :b])))))

(use-fixtures :once test-fixture)
