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

(use-fixtures :once test-fixture)
