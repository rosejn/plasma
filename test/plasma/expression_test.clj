(ns plasma.expression-test
  (:use [plasma expression]
        clojure.test)
  (:require [plasma.query :as q]))

(comment deftest expr-test
  (-> (q/path [f [:foo]
               b [f :bar]])
    (where (= (* 20 (:score 'f)) (/ (:limit 'b) 2)))))
