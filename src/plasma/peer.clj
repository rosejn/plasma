(ns plasma.peer
  (:use [lamina core]
        [aleph object]
        [plasma core query]
        jiraph.graph)
  (:require [logjam.core :as log]))

(log/channel :peer :debug)

(def DEFAULT-PORT 4242)

(def MAX-POOL-SIZE 50)
(def IDEAL-POOL-SIZE 40)

(defonce connection-pool* (atom {}))

(def DELIMITER "\0")

(defn current-time []
  (System/currentTimeMillis))

(defn- flush-connection-pool []
  (let [to-drop (- (count @connection-pool*) IDEAL-POOL-SIZE)
        conns (drop to-drop (sort-by :last-used (vals @connection-pool*)))]
    (reset! connection-pool*
            (reduce #(assoc %1 [(:host %2) (:port %2)] %2) conns))))

(defn clear-connection-pool []
  (reset! connection-pool* {}))

(defn refresh-connection [con]
  (let [new-con (assoc con :last-used (current-time))]
    (swap! connection-pool* #(assoc % [(:host con) (:port con)] new-con))
    (if (> (count @connection-pool*) MAX-POOL-SIZE)
      (flush-connection-pool))
    new-con))

(defn- create-connection [host port]
  (let [chan (wait-for-result (object-client {:host host :port port}))
        con {:host host
             :port port
             :channel chan}]
    (refresh-connection con)))

(defn peer-connection
  "Returns a connection for the given host and port.
  Uses a cached connection when available."
  [host port]
  (if-let [con (get @connection-pool* [host port])]
    (refresh-connection con)
    (create-connection host port)))

(defn- query-handler [graph q]
  ;(log/to :peer "query-handler q[" (type q) "]: " q)
  (let [resp (cond
               (query? q) (with-graph graph (query q))
               (uuid? q)  (with-graph graph  (find-node q))
               (= :ping q) :pong
               :default nil)]
    ;(log/to :peer "query result:" resp)
    resp))

(defn server-handler [graph ch client-info]
  (log/to :peer "Client connected: " client-info)
  (receive-all ch
    (fn [req]
      (log/to :peer "server-handler[" (type req) "]: " req)
      (when req
        (try
          (log/to :peer "got request:\n" req)
          (let [response (query-handler graph req)]
            (log/to :peer "response: " response)
            (enqueue ch response))
        (catch Exception e
          (log/to :peer "server error")
          (log/to :peer "------------")
          (log/to :peer "req from " client-info ": " req)
          (log/to :peer "caused exception: " e (.printStackTrace e))))))))

(defn query-server
  "Listens on port responding to queries against graph.
  Returns a function that will stop the server when called."
  [graph port]
  (log/to :peer "query-server: " port)
  (start-object-server 
    (partial server-handler graph)
    {:port port}))

(defn remote-query
  "Send a query over the given connection.  Returns a constant channel
  that will get the result of the query when it arrives."
  [con query]
  (let [result (constant-channel)
        chan (:channel con)]
    (log/to :peer "sending remote-query: " (type query))
    (enqueue chan query)
    (receive chan #(enqueue result %))
    result))

(defmethod peer-sender "plasma"
  [url]
  (let [url (url-map url)]
    (partial remote-query (peer-connection (:host url) (:port url)))))

(defn- init-peer-graph
  []
  (when-not (find-node ROOT-ID)
    (let [root (node ROOT-ID :label :root)
          net  (node :label :net)]
      (edge ROOT-ID net :label :net))))

(defn peer
  "Create a new peer using a graph database located at path, optionally
  specifying the port number to listen on."
  [path & [port]]
  (let [port (if port port DEFAULT-PORT)
        graph {:graph (layer path)}]
    (log/to :peer "peer path:" path " port:" port)
       {:type :peer
        :server (atom (query-server graph port))
        :port port
        :connections {}
        :graph graph}))

(defn peer-listen
  "Start listening for connections on the given peer."
  [p]
  (reset! (:server p) (query-server (:graph p) (:port p))))

(defn peer-close
  "Stop listening for incoming connections."
  [p]
  (@(:server p))
  (reset! (:server p) nil))

