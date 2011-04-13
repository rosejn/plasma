(ns benchmark.traversal
  (:use [plasma util core connection peer viz]
        [clojure test stacktrace]
        test-utils)
  (:require [logjam.core :as log]
            [plasma.query :as q]))

(defn criss-cross-graph []
  (let [root (root-node)
        [a b c d e f g h] (repeatedly 8 make-node)]
    (make-edge root a :a)
    (make-edge root b :a)
    (make-edge a c :b)
    (make-edge b c :b)
    (make-edge c d :c)
    (make-edge c e :c)
    (make-edge d f :d)
    (make-edge e f :d)
    (make-edge f g :e)
    (make-edge f h :e)))

(defn visited-benchmark []
  (let [p (peer "db/p1" {:port 1234})]
    (try
      (with-peer-graph p
        (clear-graph)
        (criss-cross-graph))
      (query p (q/path [:a :b :c :d :e]))
      (finally
        (close p)))))

