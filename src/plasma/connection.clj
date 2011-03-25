(ns plasma.connection
  (:use [aleph object]
        [plasma config util presence rpc])
  (:require [logjam.core :as log]
						[lamina.core :as lamina]))

(log/system :con)

(def *connection-timeout* 2000)
(def *cache-keep-ratio* 0.8)

(defonce connection-cache* (atom {}))

(defprotocol IClosable
  (close [this]))

(defprotocol IConnection
  (request       
    [con method params] 
    "Send a request over this connection. Returns a constant channel 
    that will receive the single result message.")

  (notify        
    [con method params] 
    "Send a notification over this connection.")
  
  (stream-channel
    [con method params]
    "Open a stream channel on this connection.  Returns a channel that can be used bi-directionally.")

  (request-channel
    [con] 
    "Returns a channel for incoming requests.  The channel will receive 
    [ch request] pairs, and the rpc-response or rpc-error enqueued on 
    ch will be sent as the response.")

  (notification-channel 
    [con] 
    "Returns a channel for incoming notifications.")
  
  (error-channel
    [con] 
    "Returns a channel for errors (disconnect, etc.)"))

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

(defrecord PeerConnection
  [url chan]
  IConnection

  (request 
    [this method params]
    (let [id (uuid)
          res (response-channel chan id)]
      (log/to :con "request id: " id)
      (lamina/enqueue chan (rpc-request id method params))
      res))

  (notify 
    [this method params]
    (lamina/enqueue chan (rpc-notify method params)))

  (request-channel
    [this]
    (log/to :con "request-channel...")
    (lamina/map* (fn [request] [chan request])
                 (type-channel chan :request)))

  (notification-channel
    [this] 
    (type-channel chan :notification))

  (error-channel
    [this] 
    nil)

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
     (lamina/receive-all (type-channel chan :response) #(log/to :con "client msg: " %))
     (PeerConnection. url chan)))
  ([ch {:keys [remote-addr]}]
   (PeerConnection. (str "plasma://" remote-addr) ch)))

(defn- lru-flush
  "Remove the least recently used connections."
  [connections n-to-drop]
  (let [sorted  (sort-by :last-used connections)
        to-drop (take n-to-drop sorted)
        to-keep (drop n-to-drop sorted)]
    [to-keep to-drop]))

(def *cache-policy* lru-flush)

(defn- flush-connection-cache
  []
  (let [n-to-drop (- (count @connection-cache*) 
                   (* (config :connection-cache-limit) 
                      *cache-keep-ratio*))]
    (swap! connection-cache*
           (fn [conn-map]
             (let [[to-keep to-drop] (*cache-policy* (vals conn-map) n-to-drop)]
               (doseq [con to-drop]
                 (close con))
               (zipmap (map :url to-keep) to-keep))))))

(defn clear-connection-cache []
  (doseq [con (vals @connection-cache*)]
    (close con))
  (reset! connection-cache* {}))

(defn refresh-connection
  [con]
  (let [con (assoc con :last-used (current-time))]
    (swap! connection-cache* assoc (:url con) con)
    (if (>= (count @connection-cache*) (config :connection-cache-limit))
      (flush-connection-cache))
    con))

(defn connection
  "Returns a connection to the peer listening at URL, using a cached
  connection when available."
  [url]
  (let [con (or (get @connection-cache* url)
                (make-connection url))]
    (refresh-connection con)))

(defn register-connection
  "Register a channel that was initiated externally so we can use
  it for future outgoing requests. Returns an IConnection."
  [ch client-info]
  (refresh-connection (make-connection ch client-info)))

(defprotocol IConnectionListener
  (connection-channel 
    [this] 
    "Returns a channel that will receive a Connection for each new incoming
    connection."))

(defrecord ConnectionListener
  [server port chan]
  IConnectionListener
  (connection-channel 
    [this] 
    (lamina/fork chan))

  IClosable
  (close 
    [this]
    (server)
    (lamina/close chan)))

(defn connection-listener
  "Listen on a port for incoming connections."
  [port]
  (let [con-chan (lamina/channel)
        s (start-object-server
            (fn [chan client-info]
              (log/to :con "connection: " client-info)
              (lamina/enqueue con-chan (register-connection chan client-info)))
            {:port port})]
    (ConnectionListener. s port con-chan)))
    
