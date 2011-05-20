(ns plasma.query.plan-test
  (:use
    [plasma.query core plan exec]
    [clojure test stacktrace]
    test-utils)
  (:require [logjam.core :as log]))

(def TIMEOUT 200)

(use-fixtures :each test-fixture)

(deftest optimizer-test
  (let [plan (-> (path [sesh [:sessions :session]
                        synth [sesh :synth]])
               (where (= (:label 'synth) :kick))
               (project ['sesh :label]))
        tree (query-tree plan TIMEOUT)
        optimized (optimize-plan plan)
        opt-tree (query-tree optimized TIMEOUT)]
    ;(print-query plan)
    ;(print-query optimized)
    (run-query tree {})
    (run-query opt-tree {})
    (is (= (doall (query-tree-results tree))
           (doall (query-tree-results opt-tree))))))

