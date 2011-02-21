(ns plasma.peer
  (:use [lamina core connections]
        [aleph object]
        [plasma util core query]
        jiraph.graph)
  (:require [logjam.core :as log]))

(log/channel :peer :debug)
;(log/console :peer)

(def DEFAULT-PORT 4242)

(def MAX-POOL-SIZE 50)
(def IDEAL-POOL-SIZE 40)

(defonce peer-pool* (atom {}))

(def DELIMITER "\0")

(defn current-time []
  (System/currentTimeMillis))

(defn- close-peer-con
  [con]
  (close con))
;  (close-connection con))

(defn- flush-peer-pool
  []
  {:post [(every? #(contains? % :connection) (vals @peer-pool*))]}
  (let [to-drop (- (count @peer-pool*) IDEAL-POOL-SIZE)
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
    (if (> (count @peer-pool*) MAX-POOL-SIZE)
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

(defn peer
  "Returns a peer for the given host and port.
  Uses a cached connection when available."
  [host port]
  (let [con (if-let [c (get @peer-pool* [host port])]
              (refresh-peer c)
              (peer-cache host port))]
    (assoc con :type :peer)))

(defn peer-dispatch [graph ch client-info]
  (log/to :peer "[peer-dispatch] new client: " client-info)
  (receive-all ch
    (fn [req]
      (when req
        (log/to :peer "[peer-dispatch] req-id: " (:id req))
        (let [id (:id req)
              msg (:body req)]
          (try
            (let [response 
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

(defn- peer-server
  "Listens on port responding to queries against graph.
  Returns a function that will stop the server when called."
  [graph port]
  (log/to :peer "[peer-server] starting on port: " port)
  (start-object-server
    (partial peer-dispatch graph)
    {:port port}))

; TODO: handle (re-)connection errors here...?
(defn- peer-connection
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
        chan (peer-connection peer)
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
        chan   (peer-connection peer)]
    (enqueue chan req)
    chan))

(defmethod peer-sender "plasma"
  [url]
  (let [{:keys [host port]} (url-map url)]
    (partial peer-sub-query (peer host port))))

(defn- init-peer-graph
  []
  (when-not (:edges (find-node (root-node)))
    (let [net  (node :label :net)]
      (edge (root-node) net :label :net))))

(defn local-peer
  "Create a new peer using a graph database located at path, optionally
  specifying the port number to listen on."
  [path & [port]]
  (let [port (if port port DEFAULT-PORT)
        g (graph path)]
    (log/to :peer "[peer] path:" path " port:" port)
       {:type :local-peer
        :server (atom (peer-server g port))
        :port port
        :graph g}))

(defn peer-listen
  "Start listening for connections on the given peer."
  [p]
  (reset! (:server p) (peer-server (:graph p) (:port p))))

(defn peer-close
  "Stop listening for incoming connections."
  [p]
  (cond
    (contains? p :server) (do
                            (@(:server p))
                            (reset! (:server p) nil))
    (contains? p :connection) (do
                                (close-connection (:connection p)))))


