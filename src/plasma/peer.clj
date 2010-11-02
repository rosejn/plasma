(ns plasma.peer
  (:use [aleph core tcp formats]
        jiraph
        [plasma core query])
  (:require [clojure.contrib.logging :as log]))

(def DEFAULT-PORT 4242)

(def MAX-POOL-SIZE 50)
(def IDEAL-POOL-SIZE 40)

(def connection-pool* (atom {}))

(defn current-time []
  (System/currentTimeMillis))

(defn- flush-connection-pool []
  (let [to-drop (- (count @connection-pool*) IDEAL-POOL-SIZE)
        conns (drop to-drop (sort-by :last-used (vals @connection-pool*)))]
    (reset! connection-pool*
            (reduce #(assoc %1 [(:host %2) (:port %2)] %2) conns))))

(defn refresh-connection [con]
  (let [new-con (assoc con :last-used (current-time))]
    (swap! connection-pool* #(assoc % [(:host con) (:port con)] new-con))
    (if (> (count @connection-pool*) MAX-POOL-SIZE)
      (flush-connection-pool))
    new-con))

(defn- create-connection [host port]
  (let [chan (wait-for-pipeline (tcp-client {:host host :port port}))
        con {:host host
             :port port
             :channel chan}]
    (refresh-connection con)))

(defn peer-connection [host port]
  (if-let [con (get @connection-pool* [host port])]
    (refresh-connection con)
    (create-connection host port)))

(defn- query-handler [graph q]
  (let [resp (with-graph graph
                         (cond
                           (query? q) (run-query q)
                           (uuid? q)  (find-node q)
                           (= "ping" q) "pong"
                           :default nil))]
    (println "query result:" resp)
    resp))

(defn server-handler [graph ch client-info]
  (receive-all ch
    (fn [cb]
      (when cb
        (let [msg-str (byte-buffer->string cb)]
          (println "server-handler: " msg-str)
          (try
            (let [msg (read-string msg-str)]
              (if-let [resp (query-handler graph msg)]
                (enqueue ch resp)))
            (catch Exception e
              (println "server error")
              (println "------------")
              (println "msg from " client-info ": " msg-str)
              (println "caused exception: " e (.printStackTrace e)))))))))

(defn query-server [graph port]
  "Listens on port responding to queries against graph.
  Returns a function that will stop the server when called."
  (start-tcp-server (partial server-handler graph)
                    {:port port
                     :delimiters ["\n" "\r\n"]}))

; TODO: Use a timeout with read-channel so we can handle dropped queries
(defn remote-query [con query]
  (let [result (constant-channel)
        chan (:channel con)]
    (enqueue chan (prn-str query))
    (receive chan #(enqueue result (byte-buffer->string %)))
    result))

(defmethod peer-sender "plasma" [url]
  (let [url (url-map url)]
    (partial remote-query (peer-connection (:host url) (:port url)))))

(defn- init-peer-graph []
  (when-not (find-node ROOT-ID)
    (let [root (node ROOT-ID :label :root)
          net  (node :label :net)]
      (edge ROOT-ID net :label :net))))

(defmacro peer [path & [port]]
  (let [port (if port port DEFAULT-PORT)]
    `(do
       (jiraph/defgraph SELF#
                 :path ~path :create true
                 ~'(layer :graph :auto-compact true))
       (jiraph/set-graph! SELF#)
       {:type :peer
        :server (atom (query-server SELF# ~port))
        :port ~port
        :connections {}
        :graph SELF#})))

(defn peer-listen [p]
  (reset! (:server p) (query-server (:graph p) (:port p))))

(defn peer-close [p]
  (@(:server p))
  (reset! (:server p) nil))

(defn log-sample []
  (log/info "This is a test..."))

