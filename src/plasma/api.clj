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

(defprotocol IClosable
  (close [this]))

(defprotocol IQueryable
  ; TODO: change this to find-node once core and query are refactored...
  (get-node
    [this id]
    "Lookup a node by UUID.")

  (construct
    [this spec]
    "Construct a graph based on a spec.")

  (query
    [this q] [this q params] [this q params timeout]
    "Issue a query against the peer's graph.")

  (query-channel
    [this q] [this q params]
    "Issue a query against the graph and return a channel onto which the
    results will be enqueued.")

  (recur-query
    [this q] [this pred q] [this pred q params]
    "Recursively execute a query.")

  (iter-n-query
    [this iq] [this n q] [this n q params]
    "Execute a query iteratively, n times. The output of one execution is
    used as the input to the iteration.")
)

(defprotocol IPeer
  (peer-id [this] "Get the UUID for this peer."))

(defprotocol IConnection
  (request
    [con method params]
    "Send a request over this connection. Returns a result-channel
    that will receive a single result message, or an error.")

  (request-channel
    [con]
    "Returns a channel for incoming requests.  The channel will receive
    [ch request] pairs, and the rpc-response or rpc-error enqueued on
    ch will be sent as the response.")

  (send-event
    [con id params]
    "Send an event over this connection.")

  (event-channel
    [con] [con id]
    "Returns a channel for incoming events.  If an ID is passed only incoming
    events with this ID will be enqueued onto the returned channel.")

  (stream
    [con method params]
    "Open a stream channel on this connection.  Returns a channel that can be
    used bi-directionally.")

  (stream-channel
    [con]
    "Returns a channel for incoming stream requests.  The channel will receive
    [ch request] pairs, and the ch can be used as a named bi-direction stream.")

  (on-closed
    [con handler]
    "Register a handler to be called when this connection is closed."))


