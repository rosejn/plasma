(ns benchmark.graph
  (:use plasma.core
        clojure.stacktrace
        [clojure.java.io :only (reader)]
        [clojure.contrib.str-utils2 :only (split)])
  (:require [logjam.core :as log]
            [jiraph.graph :as jiraph]
            [plasma.query.core :as q]
            [clojure.set :as set])
  (:import [java.io FileInputStream BufferedInputStream
            BufferedReader InputStreamReader File]
           [java.util.zip GZIPInputStream]))

(def test-lines
"# comments at the top
# comments at the top
# comments at the top
23 43
52 532
43 12
234 53
12 78
234 43
1  43
43      132
12 79798
432 7
213 8")

(defn gz-reader [path]
  (-> path File. FileInputStream. GZIPInputStream. InputStreamReader. BufferedReader.))

(defn edge-pair-seq
  [lines]
  (let [no-comments (filter #(not= \# (first %)) lines)
        tokenized   (map #(split % #"\s+") no-comments)
        parsed      (map #(Integer/parseInt %) (flatten tokenized))]
    (partition 2 parsed)))

(defn load-graph
  [g edges node-map]
  (let [all-nodes (set (flatten (gnutella-edges)))
        src-nodes (set (map first (gnutella-edges)))
        plain-nodes (set/difference all-nodes src-nodes)]
    (jiraph/with-graph g
      (clear-graph)
      (doseq [n plain-nodes]
        (make-node {:id (get node-map n) :score (rand)}))
      (loop [edges edges
             n-count 0
             e-count 0]
        (if-not (empty? edges)
          (let [src (ffirst edges)
                [src-edges more] (split-with #(= src (first %)) edges)
                tgts (map second src-edges)
                edge-map (zipmap tgts
                                 (repeat {:label :peer}))
                node {:id (get node-map src) :edges edge-map :score (rand)}]
            (make-node node)
            (recur more (inc n-count) (+ e-count (count tgts))))
          [(+ (count plain-nodes) n-count) e-count])))))

(def mem-graph  (open-graph))
(def disk-graph (open-graph "db/gnutella"))

(def gnutella "data/p2p-Gnutella08.txt.gz")

(defn gnutella-edges
  []
  (edge-pair-seq (line-seq (gz-reader gnutella))))

(def node-map (doall (zipmap (set (flatten (gnutella-edges)))
                             (repeatedly uuid))))

(defn mem
  []
  (load-graph mem-graph (gnutella-edges) node-map))

(defn disk
  []
  (load-graph disk-graph (gnutella-edges) node-map))

(defn peer-degrees
  [ids]
  (map first (map #(q/query (q/count* (q/path [% :peer]))) ids)))

(defn peer-stats
  [g]
  (jiraph/with-graph mem-graph
    (let [degs (peer-degrees (vals node-map))]
      {:average (average degs) :max (max degs) :min (min degs)})))


