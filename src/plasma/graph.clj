(ns plasma.graph
  (:use [plasma util]
        [clojure.contrib.core :only (dissoc-in)]))

(defprotocol IGraph
  (root-node
    [])

  (find-node
    [id])

  (get-edges
    [id] [id pred])

  (incoming-nodes
    [id])

  (make-node
    [id props])

  (remove-node
    [id])

  (make-edge
    [src tgt label-or-props])

  (remove-edge
    [srt tgt] [src tgt pred])

  (node-assoc
    [id & key-vals]))

;TODO: Add meta-data for :root 
(defn open-graph
  []
  {:nodes {}
   :incoming {}})

(defn make-node
  [g id props]
  (assoc-in g [:nodes id] (assoc props :id id)))

(defn remove-node
  [g id]
  (dissoc-in g [:nodes id]))

(defn make-edge
  [src tgt label-or-props]
  (let [props (if (map? label-or-props)
                label-or-props
                {:label label-or-props})]
    (assoc-in g [:nodes src :edges] tgt props)))

(defn remove-edge
  [src tgt]
  (dissoc-in g [:nodes src :edges tgt]))

(defn node-assoc
  [id & key-vals]
  (update-in g [:nodes id] #(apply assoc % key-vals)))


