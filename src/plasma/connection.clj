(ns plasma.connection
  (:use [aleph object]
        [plasma config util presence rpc])
  (:require [logjam.core :as log]
						[lamina.core :as lamina]))

(def *connection-timeout* 2000)
(def *cache-keep-ratio* 0.8)

(log/repl :con)

(defprotocol IClosable
  (close [this]))

(defprotocol IConnection
  (request
    [con method params]
    "Send a request over this connection. Returns a constant channel
    that will receive the single result message.")

  (request-channel
    [con]
    "Returns a channel for incoming requests.  The channel will receive
    [ch request] pairs, and the rpc-response or rpc-error enqueued on
    ch will be sent as the response.")

  (notify
    [con method params]
    "Send a notification over this connection.")

  (notification-channel
    [con]
    "Returns a channel for incoming notifications.")

  (stream
    [con method params]
    "Open a stream channel on this connection.  Returns a channel that can be used bi-directionally.")

  (stream-channel
    [con]
    "Returns a channel for incoming stream requests.  The channel will receive
    [ch request] pairs, and the ch can be used as a named bi-direction stream."))

(defn- type-channel
  "Returns a channel of incoming messages on chan of only the given type."
  [chan type]
  (lamina/filter*
    (fn [msg] (and (associative? msg)
                   (= type (:type msg))))
    (lamina/fork chan)))

(defn- response-channel
  "Returns a constant channel that will receive a single response
  for the request id."
  [chan id]
  (let [res-chan (lamina/constant-channel)
        res (lamina/take* 1 (lamina/filter* #(= id (:id %))
                             (type-channel chan :response)))]
    (lamina/siphon res res-chan)
    res-chan))

(defn- wrapped-stream-channel
  [chan id]
  (let [s-in-chan (lamina/map* #(:msg %)
                               (lamina/filter* #(= id (:id %))
                                  (type-channel chan :stream)))
        wrap-chan (lamina/channel)
        [snd-chan rcv-chan] (lamina/channel-pair)]
    (lamina/siphon s-in-chan rcv-chan)
    (lamina/siphon (lamina/map* (fn [msg]
                    {:type :stream
                     :id id
                     :msg msg})
                  rcv-chan)
            chan)
    (lamina/receive-all (lamina/fork snd-chan)
      #(log/to :stream "send: " %))
    (lamina/receive-all (lamina/fork rcv-chan)
      #(log/to :stream "recv: " %))

    snd-chan))

(defrecord Connection
  [url chan]
  IConnection

  (request
    [this method params]
    (let [id (uuid)
          res (response-channel chan id)]
      (lamina/enqueue chan (rpc-request id method params))
      res))

  (request-channel
    [this]
    (lamina/map* (fn [request] [chan request])
                 (type-channel chan :request)))

  (notify
    [this method params]
    (lamina/enqueue chan (rpc-notify method params)))

  (notification-channel
    [this]
    (type-channel chan :notification))

  (stream
    [this method params]
    (let [id (uuid)
          req {:type :stream-request
               :id id
               :method method
               :params params}]
      (lamina/enqueue chan req)
      (wrapped-stream-channel chan id)))

  (stream-channel
    [this]
    (lamina/map* (fn [s-req]
                   [(wrapped-stream-channel chan (:id s-req)) s-req])
                 (type-channel chan :stream-request)))

  IClosable
  (close
    [this]
    (lamina/close chan)))

(defn- make-connection
  "Returns a *new* connection to the peer listening at URL."
  ([url]
   (let [{:keys [proto host port]} (url-map url)
         client (object-client {:host host :port port})
         chan   (lamina/wait-for-result client *connection-timeout*)]
     ;(lamina/receive-all (type-channel chan :response)
     ;                    #(log/to :con "client msg: " %))
     (Connection. url chan)))
  ([ch {:keys [remote-addr]}]
   (Connection. (str "plasma://" remote-addr) ch)))


(defprotocol IConnectionCache
  "A general purpose PeerConnection cache."

  (get-connection
    [this url]
    "Returns a connection to the peer listening at URL, using a cached
    connection when available.")

  (register-connection
    [this ch url]
    "Add a new connection to the cache that will use an existing channel and
    URL.  Used to register connections initiated remotely.
    Returns a Connection.")

  (refresh-connection
    [this con]
    "Updates the usage timestamp on this connection to keep it from being
    removed from the cache.")

  (purge-connections
    [this]
    "Apply the cache policy to the current set of connections, possibly
    removing old or unused connections to make space for new ones.  This is
    called automatically so it should normally not need to be called manually.")

  (clear-connections
    [this]
    "Remove all connections from the cache.")

  (connection-count
    [this]
    "Get the current number of connections in the cache."))

(defrecord ConnectionCache
  [connections* flush-fn]

  IConnectionCache

  (get-connection
    [this url]
    (let [con (or (get @connections* url)
                  (make-connection url))]
      (refresh-connection this con)))

  (register-connection
    [this ch client-info]
    (refresh-connection this (make-connection ch client-info)))

  (refresh-connection
    [this con]
    (let [con (assoc con :last-used (current-time))]
      (swap! connections* assoc (:url con) con)
      (when (>= (connection-count this) (config :connection-cache-limit))
        (purge-connections this))
      con))

  (clear-connections
    [this]
    (doseq [con (vals @connections*)]
      (close con))
    (reset! connections* {}))

  (purge-connections
    [this]
    (let [n-to-drop (- (connection-count this)
                       (* (config :connection-cache-limit)
                          *cache-keep-ratio*))]
      (swap! connections*
             (fn [conn-map]
               (let [[to-keep to-drop] (flush-fn (vals conn-map) n-to-drop)]
                 (doseq [con to-drop]
                   (close con))
                 (zipmap (map :url to-keep) to-keep))))))

  (connection-count
    [this]
    (count @connections*)))

(defn- lru-flush
  "Remove the least recently used connections."
  [connections n-to-drop]
  (let [sorted  (sort-by :last-used connections)
        to-drop (take n-to-drop sorted)
        to-keep (drop n-to-drop sorted)]
    [to-keep to-drop]))

; TODO: support options for the cache size and timeout...
; * make connections asynchronous and call a callback or something
(defn connection-manager
  "Returns a connection cache that can be used to efficiently manage a large
  number of network connections, where the least-recently-used connections
  are dropped as new connections are made."
  []
  (ConnectionCache. (atom {}) lru-flush))

(defprotocol IConnectionListener
  (on-connect
    [this handler]
    "Register a handler function to be called on each incoming connection.  The
    handler will be passed a Connection."))

(defrecord ConnectionListener
  [server port chan]

  IConnectionListener
  (on-connect
    [this handler]
    (lamina/receive-all 
      (lamina/filter* #(not (nil? %)) chan) 
      handler))

  IClosable
  (close
    [this]
    (server)
    (lamina/close chan)))

(defn connection-listener
  "Listen on a port for incoming connections, automatically registering them."
  [manager port]
  (let [connect-chan (lamina/channel)
        s (start-object-server
            (fn [chan client-info]
              ;(log/to :con "connection: " client-info)
              (let [con (register-connection manager chan client-info)]
                (lamina/enqueue connect-chan con)))
            {:port port})]
    (ConnectionListener. s port connect-chan)))

