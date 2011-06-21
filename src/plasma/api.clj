(ns plasma.api)

;; Core API
(comment defprotocol IGraph
  (root-node-id [g])
  (make-node [g] [g arg] [g id props])
  (remove-node [g id])
  (find-node [g id])
  (assoc-node [id & key-vals])
  (make-edge [g src tgt args])
  (remove-edge [g src tgt] [g src tgt pred])
  (assoc-edge [g src tgt pred & key-vals])
  (get-edges [g id] [g id pred])
  (incoming-nodes [g id])
  (clear-graph [g]))

(comment defprotocol IGraphEventSource
  (node-event-channel [g id])
  (edge-event-channel [g src-id] [g src-id pred]))

