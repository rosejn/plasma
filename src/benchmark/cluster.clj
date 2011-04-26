(ns benchmark.cluster
  (:use [plasma util core url connection peer bootstrap route]
        [clojure test stacktrace]
        test-utils)
  (:require [logjam.core :as log]
            [plasma.query :as q]))

(defn cluster-ids
  [n-clusters size]
  (let [start-id 100
        step (* size 1000)]
    (for [i (range n-clusters) j (range size)]
      (+ start-id (* step i) j))))

(defn cluster-peers [n-cl cl-size]
  (let [ids (cluster-ids n-cl cl-size)
        strapper (bootstrap-peer {;:path "db/strapper" 
                                  :port 1234})
        strap-url (plasma-url "localhost" 1234)
        peers (make-peers (* n-cl cl-size) (+ 2000 (rand-int 10000))
                          (fn [i]
                            (clear-graph)
                            (make-edge ROOT-ID (make-node {:name :net}) :net)
                            (make-edge ROOT-ID (make-node {:value (nth ids i)}) :data)))]
    (doseq [p peers]
      (future (bootstrap p strap-url)))
    [strapper peers]))

(defn gather-peers
  [p n]
  (doseq [{id :id url :proxy} (random-walk-n p n)]
    (unless (get-node p id)
            (add-peer p id url))))

(defn trim-peers
  [p n]
  (let [peers (query p (->
                         (q/path [peer [:net :peer]
                                  data [peer :data]])
                         (q/sort* 'data :value)
                         (q/project [peer] [data :value])))]
    peers))

(def N-CLUSTERS 2)
(def CLUSTER-SIZE 3)

(defn cluster-benchmark
  []
  (let [[strapper peers] (cluster-peers N-CLUSTERS CLUSTER-SIZE)]
    (Thread/sleep (* 2 BOOTSTRAP-RETRY-PERIOD))
    (try
      (doseq [p peers]
        (future (gather-peers p 12)))
      (Thread/sleep 3000)
      (doseq [p peers]
        (future (gather-peers p 12)))
      [strapper peers]
      #_(finally
        (close strapper)
        (close-peers peers)))))

;(doseq [p peers] (gather-peers p 12))
