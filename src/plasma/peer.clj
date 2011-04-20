(ns plasma.peer
  (:use [plasma config util core connection network presence rpc]
        clojure.stacktrace)
  (:require [logjam.core :as log]
            [lamina.core :as lamina]
            [jiraph.graph :as jiraph]
            [plasma.query :as q]))

(defmacro with-peer-graph [p & body]
  `(let [g# (:graph ~p)]
     (locking g#
     (if (= jiraph/*graph* g#)
       (do ~@body)
       (jiraph/with-graph g# ~@body)))))

(defprotocol IQueryable
  ; TODO: change this to find-node once core and query are refactored...
  (get-node
    [this id]
    "Lookup a node by UUID.")

  (link
    [this src node-map edge-props]
    "Create an edge from src to a new node with the given edge properties.")

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

; Add support for specifying the binding variable name, rather than
; only using the ROOT-ID.

(comment
  query-iter-n
  query-iter-until
  query-iter-while
  query-recur-n
  query-recur-until
  query-recur-while

  (iter-until-query
    [this q] [this pred q] [this pred q params]
  "Execute a query iteratively until the p")

  (recur-n-query
    [this iq] [this n q] [this n q params])

  (recur-until-query
    [this q] [this pred q] [this pred q params]))


(def *manager* nil)
(def DEFAULT-HTL 50)

(defrecord PlasmaPeer
  [manager graph port listener status options] ; peer-id, port, max-peers

  IQueryable
  (get-node
    [this id]
    (with-peer-graph this (find-node id)))

  (link
    [this src node-props edge-props]
    (with-peer-graph this
      (let [n (make-node node-props)]
        (make-edge src n edge-props)
        n)))

  (query
    [this q]
    (query this q {}))

  (query
    [this q params]
    (query this q params q/MAX-QUERY-TIME))

  (query
    [this q params timeout]
    (binding [*manager* manager]
      (with-peer-graph this
        (q/query q params timeout))))

  (query-channel
    [this q]
    (query-channel this q {}))

  (query-channel
    [this q params]
    (binding [*manager* manager]
      (with-peer-graph this
        (q/query-channel q params))))

  (recur-query
    [this pred q]
    (recur-query this pred q {}))

  (recur-query
    [this pred q params]
    (let [params (merge (:params q) params)
          iplan (assoc q
                       :type :recur-query
                       :src-url (public-url port)
                       :pred pred
                       :recur-count count
                       :htl DEFAULT-HTL
                       :params params)]
      (recur-query this iplan)))

  (recur-query
    [this q]
     (comment let [q (map-fn q :recur-count dec)
           res-chan (query-channel q (:params q))]
       (lamina/on-closed res-chan
         (fn []
           (let [res (lamina/channel-seq res-chan)]
             ; Send the result back if we hit the end of the recursion
             (if (zero? (:recur-count q))
               (let [src-url (:src-url q)
                     query-id (:id q)
                     con (get-connection manager (:src-url q))]
                 (send-event con query-id res))

               ; or recur if not
               (doseq [n res]
                 (if (proxy-node? n)
                   (peer-recur-query
                 (receive-all res-chan
                              (fn [v]
                                (if (proxy-node? v)
                                  (peer-recur q v)
                                  (recur* q v)))))))))))))

  ; TODO: Support binding to a different parameter than the ROOT-ID
  ; by passing a {:bind 'my-param} map.
  (iter-n-query
    [this n q]
    (iter-n-query this n q {}))

  (iter-n-query
    [this n q params]
     (let [iplan (assoc q
                        :type :iter-n-query
                        :src-url (public-url port)
                        :iter-n n
                        :htl DEFAULT-HTL
                        :iter-params params)]
     (iter-n-query this iplan)))

  (iter-n-query
    [this q]
    (let [final-res (lamina/channel)
           iter-fn (fn iter-fn [q]
                     (log/to :peer "iter-n: " (:iter-n q))
                     (let [plan (update-in q [:iter-n] dec)
                           plan (update-in plan [:htl] dec)
                           res-chan (query-channel this plan (:iter-params plan))]
                       (lamina/on-closed res-chan
                         (fn []
                           (cond
                             (zero? (:iter-n plan))
                             (lamina/siphon res-chan final-res)

                             (zero? (:htl plan))
                             (lamina/enqueue final-res
                                             {:type :error
                                              :msg :htl-reached})

                             :default
                             (let [res (lamina/channel-seq res-chan)
                                   params (assoc (:iter-params plan) ROOT-ID res)
                                   plan (assoc plan :iter-params params)]
                               (log/to :peer "--------------------\n"
                                       "iter-fn result: "
                                       (seq res)
                                       "\n--------------------------\n")

                               (iter-fn plan)))))))]
       (iter-fn q)
       final-res))

  IConnectionListener
  (on-connect
    [this handler]
    (on-connect listener handler))

  IClosable
  (close
    [this]
    (close listener)
    (when (:internal-manager options)
      (clear-connections manager))
    (reset! status :closed)))

; TODO: FIX ME.  register-connection needs a channel and a url
(defn- setup-presence-listener
  [p]
  (lamina/receive-all (presence-listener)
    (fn [{:keys [peer-id peer-port peer-host]}]
      (register-connection (:manager p) peer-host peer-port))))

(defmulti rpc-handler
  "A general purpose rpc multimethod."
  (fn [peer req] (:method req)))

(defmethod rpc-handler 'get-node
  [peer req]
  (get-node peer (first (:params req))))

(defmethod rpc-handler 'query
  [peer req]
  (apply query peer (:params req)))
;  (query peer (first (:params req))))

(defn- request-handler
  [peer [ch req]]
  (log/format :peer "request-handler[%s]: %s" (:id req) (:method req))
  (try
    (let [res (rpc-handler peer req)
          res (if (seq? res)
                (doall res)
                res)
          rpc-res (rpc-response req res)]
      (lamina/enqueue ch rpc-res))
    (catch Exception e
      (log/to :peer "error handling request!\n------------------\n"
              (with-out-str (print-cause-trace e)))
      (.printStackTrace e)
      (lamina/enqueue ch
        (rpc-error req "Exception occured while handling request." e)))))

(defmulti stream-handler
  "A general purpose stream multimethod."
  (fn [peer ch req] (:method req)))

(defmethod stream-handler 'query-channel
  [peer ch req]
  (log/to :peer "stream-handler query-channel: " req)
  (let [res-chan (apply query-channel peer (:params req))]
    (lamina/siphon res-chan ch)))

(defn- stream-request-handler
  [peer [ch req]]
  (log/to :peer "stream-request: " (:id req))
  (try
    (stream-handler peer ch req)
    (catch Exception e
      (log/to :peer "error handling stream request!\n"
              "-------------------------------\n"
              (with-out-str (print-cause-trace e))))))

(defn handle-peer-connection
  "Hook a connection up to a peer so that it can receive queries."
  [peer con]
  (log/to :peer "handle-peer-connection new-connection: " (:url con))

  (lamina/receive-all (lamina/filter* #(not (nil? %))
                                      (lamina/fork (:chan con)))
    (fn [msg] (log/to :peer "incoming request: " msg)))

  (lamina/receive-all (request-channel con)
                      (partial request-handler peer))
  (lamina/receive-all (stream-channel con)
                      (partial stream-request-handler peer)))

(defn peer
  "Create a new peer using a graph database located at path, optionally
  specifying the port number to listen on."
  ([path] (peer path {}))
  ([path options]
   (let [port (get options :port (config :peer-port))
         [manager options] (if (:manager options)
                             [(:manager options) options]
                             [(connection-manager)
                              (assoc options :internal-manager true)])
         g (open-graph path)
         listener (connection-listener manager (config :protocol) port)
         status (atom :running)
         p (PlasmaPeer. manager g port listener status options)]
     (on-connect p (partial handle-peer-connection p))
     (when (:presence options)
       (setup-presence-listener p))
     p)))

(defn peer-connection
  "Returns a connection to a remote peer reachable by url, using the local peer p's
  connection manager."
  [p url]
  (get-connection (:manager p) url))

(defn peer-get-node
  "Lookup a node by ID on a remote peer."
  [con id]
  (let [res (lamina/constant-channel)]
    (lamina/receive (request con 'get-node [id])
                    #(lamina/enqueue res (:result %)))
    res))

(defn peer-link
  [con src node-map edge-props])

; TODO: Return a result channel and enqueue an error if we fail
(defn peer-query
  "Send a query to the given peer.  Returns a constant channel
  that will get the result of the query when it arrives."
  ([con q]
   (peer-query con q {}))
  ([con q params]
   (peer-query con q params q/MAX-QUERY-TIME))
  ([con q params timeout]
   (wait-for (lamina/map* :result (request con 'query [q params]))
               timeout)))

(defn peer-query-channel
  ([con q]
   (peer-query-channel con q {}))
  ([con q params]
   (stream con 'query-channel [q params])))

(defn peer-recur-query
  [con q])

(defn peer-iter-n-query
  [con q n])

(extend plasma.connection.Connection
  IQueryable
  {:get-node peer-get-node
   :link peer-link
   :query peer-query
   :query-channel peer-query-channel
   :recur-query peer-recur-query
   :iter-n-query peer-iter-n-query})

(defmethod peer-sender :default
  [url]
  (partial peer-query-channel (get-connection *manager* url)))

