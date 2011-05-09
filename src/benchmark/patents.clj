(ns benchmark.patents
  (:use [plasma util graph viz]
        [plasma.net url connection peer bootstrap route]
        [clojure test stacktrace]
        test-utils
        [clojure.java.io :only (reader)]
        [clojure.contrib.str-utils2 :only (split)])
  (:require [logjam.core :as log]
            [plasma.query.core :as q])
  (:import [java.io FileInputStream BufferedInputStream
            BufferedReader InputStreamReader File]
           [java.util.zip GZIPInputStream]))

(def test-lines
"# comment at the top
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

(defn load-patent-edges
  [path n]
  (with-open [in (gz-reader path)]
    (doseq [pair (take n (edge-pair-seq (line-seq in)))]
      (println pair))))

(defn patent-edge-map
  [path n]
  (with-open [in (gz-reader path)]
    (let [[_ _ nmap] (reduce
                 (fn [[cur-id cur-nodes node-map] [src tgt]]
                   (if (= cur-id src)
                     [cur-id (conj cur-nodes tgt) node-map]
                     [src (list tgt) (assoc node-map cur-id cur-nodes)]))
                 [nil '() {}]
                 (take n (edge-pair-seq (line-seq in))))]
      (dissoc nmap nil))))
