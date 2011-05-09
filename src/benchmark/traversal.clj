(ns benchmark.traversal
  (:use [plasma util graph viz]
        [plasma.net connection peer]
        [clojure test stacktrace]
        test-utils)
  (:require [logjam.core :as log]
            [plasma.query.core :as q]))

(defn criss-cross-graph []
  (let [root (root-node-id)
        [a b c d e f g h] (repeatedly 8 #(make-node {:value (rand)}))]
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
      (save-dot-graph (:graph p) "output/criss_cross_graph.dot")
      (let [q (-> (q/path [c [:a :b :c]
                           v [c :d :e]]
                          (where (> (:value v) 0.1)))
                (q/project [v :value]))]
        (save-dot-plan q "output/criss_cross_plan.dot")
        (query p q {} 300))
        (finally
          (close p)))))

