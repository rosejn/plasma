(ns plasma.viz
  (:use [plasma util core operator]
        jiraph.graph)
  (:require [logjam.core :as log]
            [vijual :as vij]
            [clojure (zip :as zip)]))

(log/channel :viz :debug)

(defn- tree-vecs* [q root]
  (let [{:keys [id type deps args]} (get (:ops q) root)
        label (case (:type root)
                :traverse (str "tr " (second args))
                :parameter (str "pr \"" (first args) "\"")
                (name type))
        label (str label " [" (apply str (take 4 (drop 5 id))) "]")]
    (apply vector label (map (partial tree-vecs* q) deps))))

(defn- tree-vecs [q]
  [(tree-vecs* q (:root q))])

(defn print-query
  "Print the query operator tree for a query plan."
  [plan]
  (vij/draw-tree (tree-vecs plan)))

(defn- dot-id
  [id]
  (apply str "ID_" (take 4 (drop 5 id))))

(defn- dot-edge
  [src tgt props]
  (let [label (name (:label props))]
    (println "\t\t" src " -> " tgt "[label=\"" label "\"];")))

(defn- dot-node
  [id]
  (let [n (find-node id)
        d-id (dot-id id)
        edges (get-edges id)
        label (or (:label n) d-id)]
    (println (str "\t" d-id " [label=\"" label "\"];"))
    (doseq [[tgt-id props] edges]
      (dot-edge d-id (dot-id tgt-id) props))))

(defn- dot-graph
  "Output the dot (graphviz) graph description for the given graph."
  [g]
  (with-graph g
    (let [nodes (node-ids :graph)]
      (with-out-str
        (println "digraph G {\n\tnode [shape=plaintext]")
        (doseq [n nodes]
          (dot-node n))
        (println "}")))))

(defn save-dot-graph
  [g path]
  (spit path (dot-graph g)))

(defn- dot-op
  [{:keys [id type args]}]
  (let [label (case type
                :traverse (str "traverse: [" (second args) "]")
                :parameter (str "param: " (if (uuid? (first args))
                                            (apply str (drop 5 (first args)))
                                            (first args)))
                (name type))]
        (str "\t" (dot-id id) " [label=\"" label "\"]")))

(defn- dot-plan
  [plan]
  (with-out-str
    (println "digraph Plan {\n\tnode [shape=plaintext]")
    (doseq [[id op] (:ops plan)]
      (println (dot-op op))
      (doseq [edge (:deps op)]
        (println (str "\t" (dot-id id) " -> " (dot-id edge)))))
    (println "}")))

(defn save-dot-plan
  [plan path]
  (spit path (dot-plan plan)))
