(ns plasma.peer
  (:use [lamina core connections]
        [aleph object]
        [plasma config util presence core query]
        jiraph.graph)
  (:require [logjam.core :as log]))

(log/channel :peer :debug)
;(log/console :peer)

(defonce peer-pool* (atom {}))

(defn- close-peer-con
  [con]
  (close con))
;  (close-connection con))

(defn- flush-peer-pool
  []
  {:post [(every? #(contains? % :connection) (vals @peer-pool*))]}
  (let [to-drop (- (count @peer-pool*) (* (config :connection-pool-size) 0.8))
        sorted  (sort-by :last-used (vals @peer-pool*))
        dropped (take to-drop sorted)
        conns (drop to-drop sorted)]
    (reset! peer-pool*
            (reduce #(assoc %1 [(:host %2) (:port %2)] %2) {} conns))
  (doseq [con (map :connection dropped)]
;    (log/to :peer "[clear-peer-pool] con: " con)
    (close-peer-con con))))

(defn clear-peer-pool []
  (doseq [con (map :connection (vals @peer-pool*))]
;    (log/to :peer "[clear-peer-pool] con: " con)
    (close-peer-con con))
  (reset! peer-pool* {}))

(defn refresh-peer [peer]
  (let [new-peer (assoc peer :last-used (current-time))]
    (swap! peer-pool* #(assoc % [(:host peer) (:port peer)] new-peer))
    (if (> (count @peer-pool*) (config :connection-pool-size))
      (flush-peer-pool))
    new-peer))

(defn- peer-cache
  "Add a peer to the peer cache, creating it if necessary."
  ([peer]
   (refresh-peer peer))
  ([host port]
   (let [con (object-client {:host host :port port})
;         con (persistent-connection #(object-client {:host host :port port}))
         ]
     {:type :peer
      :host host
      :port port
      :connection con})))

(defn peer-connection
  "Returns a peer for the given host and port.
  Uses a cached connection when available."
  ([host]
   (peer-connection host (config :peer-port)))
  ([host port]
   (let [con (if-let [c (get @peer-pool* [host port])]
               (refresh-peer c)
               (peer-cache host port))]
     (assoc con :type :peer))))

(defn peer-dispatch [peer ch client-info]
  (log/to :peer "[peer-dispatch] new client: " client-info)
  
  ; TODO: instead enqueue a peer-connection
  (enqueue (:on-connect peer) {:client-info client-info
                               :chan ch})
  (receive-all ch
    (fn [req]
      (when req
        (log/to :peer "[peer-dispatch] req-id: " (:id req))
        (let [id (:id req)
              msg (:body req)]
          (try
            (let [graph (:graph peer)
                  response
                  (cond
                    (= ROOT-ID msg)
                    (with-graph graph (root-node))

                    (uuid? msg)
                    (with-graph graph (find-node msg))

                    (= :ping msg)
                    :pong

                    (= :query (:type msg))
                    (do
                      (log/to :peer "[peer-handler] query")
                      (enqueue (:on-query peer) {:chan ch
                                                 :query msg
                                                 :client-info client-info})
                      (with-graph graph (query msg)))

                    (= :sub-query (:type msg))
                    (do
                      (log/to :peer "[peer-handler] sub-query")
                      (with-graph graph (sub-query ch msg))))]

              (let [res {:type :response :id id :body response}]
                (log/to :peer "[peer-dispatch] response: " res)
                (unless (= :sub-query (:type msg))
                        (enqueue ch res))))
            (catch Exception e
              (log/to :peer "server error")
              (log/to :peer "------------")
              (log/to :peer "req from " client-info ": " req)
              (log/to :peer "caused exception: " e (.printStackTrace e)))))))))

(defn- register-peer-connection 
  [host port])

(defn- peer-server
  "Listen on port, responding to queries against the peer graph."
  [peer]
  (log/to :peer "[peer-server] starting on port: " (:port peer))
  (let [s (start-object-server
            (partial peer-dispatch peer)
            {:port (:port peer)})
        presence-chan (presence-listener)]
    (reset! (:server peer) s)
    (receive-all presence-chan
      (fn [{:keys [peer-id peer-port peer-host]}]
        (register-peer-connection peer-host peer-port)))))

; TODO: handle (re-)connection errors here...?
(defn- get-peer-connection
  [peer]
  (if (= :local-peer (:type peer))
    (throw (Exception. "Cannot open a connection to a local peer.")))
  (wait-for-result (:connection peer) 2000))

;  (let [con ((:connection peer))
;        _ (log/to :peer "[peer-connection] con: " con)
;        res (wait-for-result con)]
;        (log/to :peer "[peer-connection] res:" res)
;    res))

(defn peer-query
  "Send a query to the given peer.  Returns a constant channel
  that will get the result of the query when it arrives."
  [peer q & [timeout]]
  (log/to :peer "[peer-query] peer: " peer)
  (let [req {:type :request :id (uuid) :body q}
        chan (get-peer-connection peer)
        result (constant-channel)
        res-filter (take* 1
                          (map* :body
                                (filter* #(= (:id req) (:id %))
                                   chan)))]
    (enqueue chan req)
    (siphon res-filter result)
    (if timeout
      (wait-for-message result timeout)
      result)))

(defn peer-sub-query
  [peer q]
  (log/to :peer "[peer-query] peer: " peer)
  (let [req {:type :request :id (uuid) :body q}
        chan   (get-peer-connection peer)]
    (enqueue chan req)
    chan))

(defmethod peer-sender "plasma"
  [url]
  (let [{:keys [host port]} (url-map url)]
    (partial peer-sub-query (peer-connection host port))))

(defn- init-peer-graph
  []
  (when-not (:edges (find-node (root-node)))
    (let [net  (node :label :net)]
      (edge (root-node) net :label :net))))

;(log/to :peer "[peer] path:" path " port:" port)

(defn local-peer
  "Create a new peer using a graph database located at path, optionally
  specifying the port number to listen on."
  [path & [port]]
  (let [port (if port port (config :peer-port))
        g (graph path)
        p {:type :local-peer
           :server (atom nil)
           :new-peers (atom nil)
           :port port
           :graph g
           :on-connect (channel)
           :on-query (channel)}]
    (peer-server p)
    p))

(defn on-peer-connect
  "Returns an event channel of new peer connections."
  [p]
  (fork (:on-connect p)))

(defn on-peer-query
  "Returns an event channel of incoming query events.
  The event is a map containing a channel to the remote peer, the query,
  and the client information for the remote peer.
    {:chan ch
    :query msg
    :client-info client-info}
  "
  [p]
  (fork (:on-query p)))

(defn peer-listen
  "Start listening for connections on the given peer."
  [p]
  (peer-server p))

(defn peer-close
  "Stop listening for incoming connections."
  [p]
  (cond
    (contains? p :server) (do
                            (@(:server p))
                            (reset! (:server p) nil))
    (contains? p :connection) (do
                                (close-connection (:connection p)))))


(comment
(defprotocol Peer
  (id [])
  (url [])
  (onNewConnection [])
  (query [])
  (numPeers [])
  (close []))

; check-live-peers: ping all peers and drop ones that don't respond
; use Upnp library to figure out public port and IP to create URL

; peer config options
; :port
; :max-peers
; :peer-id

(defprotocol PeerConnection
  (name [])
  (setKey [k])
  (request [])
  (notify [])
  (request-channel [])
  (notify-channel [])
  (disconnect-channel [])
  (error-channel [])
  (close []))

(defprotocol Queryable
  (query [])
  (sub-query [])
  (iter []))

; DataChannel
; Use to send audio, video, events, etc...
; - provides a generic object communication pipe
; - support encryption
; - support nat traversal and routing through a 3rd party
; -
; request a named channel with some args
; on-named-channel request handler
;

(defprotocol DHTClient
  (close []) ; leave?
  (put [obj])
  (get [obj])
  (delete [obj]))

; join (part of construction, or part of protocol?)
; (getSuccessor [id])

(defprotocol RandomWalkClient
  (find [key])
  (sample-query [query]))

; peer connection protocol to talk to remote peer
; id (name?): return the peer-id
; query: run a query
; get-node: return a node (or some properties) given a UUID
; get-peers: return all peers

; group communication
; join
; leave
; send-message
; on-message-received



; Example apps
;
; * text based chat (or maybe with basic swing gui)
(defprotocol ChatClient
  (onMessage [])
  (sendMessage []))

; * simple work queue system, where jobs can be added to a server and it forwards them to
; available client workers


; Buddy Service
; * looks up buddies on join, and then periodically pings them to monitor their status
; * allows for querying and opening channels to friends
; * store buddy info in local graph (proxy nodes)
(defprotocol ContactClient
  (onArrival [])
  (onDeparture [])
  (info [])
  (query [])
  (channel []))

; Distributed drawing or click command
; * simple P2P app where the user on one side clicks on the screen and the peers
; see dots or lines or something

; Given an arbitrary name return back the peer connection info
(defprotocol NamingService    ; NamingClient?
  (resolve [name]))

; Local network broadcaster (using UDP)
; - periodically broadcast a simple presence message with local peer connection info
; - listener gets called whenever a UDP packet arrives
;

; Gossip based updates of ??? document, peer list, named groups, service info?
; - gossip based publication of arbitrary object, or key/value pair?


; Super simple jSpeex audio streaming on top of Plasma
;
)
