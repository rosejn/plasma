(ns plasma.viz
  (:use [plasma core operator]
        jiraph.graph)
  (:require [logjam.core :as log]
            [vijual :as vij]
            [clojure (zip :as zip)]))

(log/channel :viz :debug)

(defn- tree-vecs* [q root]
  (let [node (get (:ops q) root)
        branch? (get op-branch-map (:type node))
        _ (log/to :viz "type: " (:type node)
                  "\nbranch: " branch?)
        children (if branch?
                   (case (:type node)
                     :join (:args node)
                     (:send
                      :select
                      :project
                      :property
                      :aggregate
                      :receive) [(first (:args node))])
                   nil)
        _ (log/to :viz "children: " children)
        label (case (:type node)
                :traverse (str "tr " (second (:args node)))
                :parameter (str "pr \"" (first (:args node)) "\"")
                (name (:type node)))
        label (str label " [" (apply str (take 4 (drop 5 (:id node)))) "]")]
    (apply vector label (map (partial tree-vecs* q) children))))

(defn- tree-vecs [q]
  [(tree-vecs* q (:root q))])

(defn print-query [plan]
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

(defn dot-graph
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

