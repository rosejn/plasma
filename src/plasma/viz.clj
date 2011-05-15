(ns plasma.viz
  (:use [plasma util graph]
        [plasma.query operator]
        jiraph.graph
        [clojure.contrib shell-out])
  (:require [logjam.core :as log]
            [vijual :as vij]
            [clojure (zip :as zip)]))

(log/channel :viz :debug)

(defn- tree-vecs* [q root]
  (let [{:keys [id type deps args]} (get (:ops q) root)
        label (case type
                :traverse (str (second args))
                :parameter (str (first args))
                (name type))
        label (str label " [" (trim-id id) "]")]
    (apply vector label (map (partial tree-vecs* q) deps))))

(defn- tree-vecs [q]
  [(tree-vecs* q (:root q))])

(defn print-query
  "Print the query operator tree for a query plan."
  [plan]
  (vij/draw-tree (tree-vecs plan)))

(defmethod clojure.core/print-method :plasma.query.core/query
  [query writer]
  (.write writer (with-out-str (print-query query))))

(defn- dot-edge
  [src tgt props options]
  (let [label (name (:label props))]
    (println (str "\t\"" src "\" -> \"" tgt "\" [label=\"" label "\"];"))))

(defn- dot-record-label
  [props options]
  (let [props (if (coll? (:node-props options))
                (select-keys props (:node-props options))
                props)
        key-vals (map
                   (fn [[k v]]
                     (str (if (keyword? k)
                            (name k)
                            k) ": "
                          (if (uuid? v)
                            (trim-id v)
                            v)))
                   props)
        prop-lbls (apply str (interpose " | " key-vals))]
    (str "[label=\"{ " prop-lbls "}\"];")))

(defn- dot-node
  [id options]
  (let [n (find-node id)
        d-id (trim-id id)
        node-props (:node-props options)
        lbl (if node-props
              (dot-record-label (dissoc n :edges) options)
              (or (name (:label n))
                  d-id))]
        ;edges (get-edges id)
        ;label (or (:label n) d-id)
        ;label (if (keyword? label)
        ;        (name label)
        ;        label)]
    (println (str "\t\"" d-id "\" [label=\"" lbl "\", shape=box]"))
    (doseq [[tgt-id props] (:edges n)]
      (dot-edge d-id (trim-id tgt-id) props options))))

(defn dot-graph
  "Output the dot (graphviz) graph description for the given graph."
  [g options]
  (with-graph g
    (let [width  (or (:width options) 6)
          height (or (:width options) 10)
          aspect (or (:aspect options) 0.5)
          nodes (filter
                  #(not= "UUID:META" %)
                  (node-ids :graph))
          node-props (:node-props options)]
      (with-out-str
        (println "digraph G {\n")
        (println "\tratio=\"" aspect "\"")
;        (println (str "\tsize=\"" width "," height "\"\n"))
        (if node-props
          (println "\tnode [shape=Mrecord]")
          (println "\tnode [shape=circle]"))
        (doseq [n nodes]
          (dot-node n options))
        (println "}")))))

(defn save-dot-graph
  ([g path] (save-dot-graph g path {}))
  ([g path options]
   (spit path (dot-graph g options))))

(def EDGE-FONT 24)
(def NODE-FONT 24)
(def OUT-FORMAT "ps")

(defn save-graph-image
  ([in out] (save-graph-image in out {}))
  ([in out options]
   (let [out-format (:out-format options OUT-FORMAT)
         edge-font-size (:edge-font-size options EDGE-FONT)
         node-font-size (:node-font-size options NODE-FONT)]
   (sh "dot"
       in
       (str "-T" out-format)
       (str "-Efontsize=" edge-font-size)
       (str "-Nfontsize=" node-font-size)
       "-o" out))))

(def APP "evince")

(defn show-dot-graph
  ([g] (show-dot-graph g {}))
  ([g options]
   (let [out-format (:out-format options OUT-FORMAT)
         app (:app options APP)
         out (str "/tmp/plasma-graph." out-format)]
     (save-dot-graph g "/tmp/plasma-graph.dot" options)
     (save-graph-image "/tmp/plasma-graph.dot" out options)
     (sh app out))))

(defn graph->ps
  [g & [options]]
   (show-dot-graph g options))

(defn graph->browser
  [g & [options]]
  (show-dot-graph g (merge options {:out-format "svg" :app "chromium-browser"})))

(defn- label-table
  [top bottom]
  (str "<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n\t"
       "<TR><TD><FONT POINT-SIZE=\"24\">" top "</FONT></TD></TR>\n"
       "<TR><TD><FONT POINT-SIZE=\"18\">" bottom "</FONT></TD></TR>\n"
       "</TABLE>>"))

(defn- dot-op
  [{:keys [id type args]}]
  (let [label (case type
                :traverse (label-table "traverse" (second args))
                :parameter (label-table "parameter"
                                (if (uuid? (first args))
                                  (trim-id (first args))
                                  (first args)))
                (str "\"" (name type) "\""))]
        (str "\t\"" (trim-id id) "\" [label=" label "]")))

(defn- dot-plan
  [plan]
  (with-out-str
    (println "digraph Plan {\n\tnode [shape=Mrecord]")
    (doseq [[id op] (:ops plan)]
      (println (dot-op op))
      (doseq [edge (:deps op)]
        (println (str "\t\"" (trim-id id) "\" -> \"" (trim-id edge) "\""))))
    (println "}")))

(defn save-dot-plan
  [plan path]
  (spit path (dot-plan plan)))
