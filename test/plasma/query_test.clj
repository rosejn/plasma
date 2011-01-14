(ns plasma.query-test
  (:use [plasma core query viz]
        [clojure test stacktrace]
        [jiraph graph]
        test-utils)
  (:require [logjam.core :as log]))

;(log/console :op)

(deftest path-test-manual
  (let [plan (path [:music :synths :synth])
        tree (query-tree plan)]
    (run-query tree {})
    (is (= #{:kick :bass :snare :hat}
           (set (map #(:label (find-node %)) 
                     (query (path [:music :synths :synth]))))))))

(deftest path-query-test
  (is (= #{:kick :bass :snare :hat}
         (set (map #(:label (find-node %)) 
                   (query (path [:music :synths :synth])))))))

(deftest where-query-test
  (is (= #{:kick :bass :snare}
         (set (map #(:label (find-node %)) 
                   (query (path [s [:music :synths :synth]]
                                (where (> (:score s) 0.3)))))))))


(use-fixtures :once test-fixture)
