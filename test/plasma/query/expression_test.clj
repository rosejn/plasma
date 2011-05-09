(ns plasma.query.expression-test
  (:use [plasma.query expression]
        clojure.test)
  (:require [plasma.query.core :as q]))

(comment deftest expr-test
  (-> (q/path [f [:foo]
               b [f :bar]])
    (where (= (* 20 (:score 'f)) (/ (:limit 'b) 2)))))
