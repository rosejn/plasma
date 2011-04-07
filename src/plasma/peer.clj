(ns plasma.peer
  (:use [plasma config util core connection network presence rpc]
        jiraph.graph
        clojure.stacktrace)
  (:require [logjam.core :as log]
            [lamina.core :as lamina]
            [plasma.query :as q]))

(defprotocol IQueryable
  (ping
    [this]
    "Simple test function to check that the peer is available.")

  ; TODO: change this to find-node once core and query are refactored...
  (node-by-uuid
    [this id]
    "Lookup a node by UUID.")

  (query
    [this q] [this q params] [this q params timeout]
    "Issue a query against the peer's graph.")

  (query-channel
    [this q] [this q params]
    "Issue a query against the graph and return a channel onto which the
    results will be enqueued.")

  (sub-query
    [this ch plan]
    "Execute a sub-query against the peer's graph, streaming the results back
    to the source of the sub-query.")

  (recur-query
    [this q] [this pred q] [this pred q params]
    "Recursively execute a query.")

  (iter-n-query
    [this iq] [this n q] [this n q params]
    "Execute a query iteratively, n times. The output of one execution is
    used as the input to the iteration."))

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
  [manager graph port listener options] ; peer-id, port, max-peers

  IQueryable
  (ping [_] 
        (log/to :peer "got ping...")
        :pong)

  (node-by-uuid
    [this id]
    (with-graph graph
                (find-node id)))

  (query
    [this plan]
    (query this plan {}))

  (query
    [this plan params]
    (binding [*manager* manager]
      (with-graph graph
                  (q/query plan params))))

  (query
    [this plan params timeout]
    (binding [*manager* manager]
      (with-graph graph
                  (q/query plan params timeout))))

  (query-channel
    [this plan]
    (query-channel this plan {}))

  (query-channel
    [this plan params]
    (binding [*manager* manager]
      (with-graph (:graph this)
                  (q/query-channel plan params))))

  (sub-query
    [this ch plan]
    (binding [*manager* manager]
      (with-graph graph
                  (q/sub-query ch plan))))

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
    [this plan]
     (comment let [plan (map-fn plan :recur-count dec)
           res-chan (query-channel plan (:params plan))]
       (lamina/on-closed res-chan
         (fn []
           (let [res (lamina/channel-seq res-chan)]
             ; Send the result back if we hit the end of the recursion
             (if (zero? (:recur-count plan))
               (let [src-url (:src-url plan)
                     query-id (:id plan)
                     con (get-connection manager (:src-url plan))]
                 (send-event con query-id res))

               ; or recur if not
               (doseq [n res]
                 (if (proxy-node? n)
                   (peer-recur-query
                 (receive-all res-chan
                              (fn [v]
                                (if (proxy-node? v)
                                  (peer-recur plan v)
                                  (recur* plan v)))))))))))))

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
    (if (:internal-manager options)
      (clear-connections manager))))

; TODO: FIX ME.  register-connection needs a channel and a url
(defn- setup-presence-listener
  [p]
  (lamina/receive-all (presence-listener)
    (fn [{:keys [peer-id peer-port peer-host]}]
      (register-connection (:manager p) peer-host peer-port))))

(defn- request-handler
  [peer [ch req]]
  (log/format :peer "request-handler[%s]: %s" (:id req) (:method req))
  (try
    (let [res (case (:method req)
                'ping (ping peer)
                'node-by-uuid (node-by-uuid peer (first (:params req)))
                'query (query peer (first (:params req))))
          res (if (seq? res)
                (doall res)
                res)
          rpc-res (rpc-response req res)]
      ;(log/to :peer "result: " rpc-res)
      (lamina/enqueue ch rpc-res))
    (catch Exception e
      (log/to :peer "error handling request!\n------------------\n"
              (with-out-str (print-cause-trace e)))
      (.printStackTrace e)
      (lamina/enqueue ch
        (rpc-error req "Exception occured while handling request." e)))))

(defn- stream-handler
  [peer [ch req]]
  (log/to :peer "stream-request: " (:id req))
  (try
    (case (:method req)
      'sub-query (sub-query peer ch (first (:params req))))
    (catch Exception e
      (log/to :peer "error handling stream request!\n"
              "-------------------------------\n"
              (with-out-str (print-cause-trace e))))))

(defn handle-peer-connection
  "Hook a connection up to a peer so that it can receive queries."
  [peer con]
  (log/to :peer "handle-peer-connection new-connection: " (:url con))
  (register-connection (:manager peer) con)
  (lamina/receive-all (lamina/filter* #(not (nil? %)) (:chan con))
    (fn [msg] (log/to :peer "incoming request: " msg)))
  (lamina/receive-all (request-channel con)
                      (partial request-handler peer))
  (lamina/receive-all (stream-channel con)
                      (partial stream-handler peer)))

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
         g (graph path)
         listener (connection-listener manager port)
         p (PlasmaPeer. manager g port listener options)]
     (on-connect p (partial handle-peer-connection p))
     (when (:presence options)
       (setup-presence-listener p))
     p)))

;TODO: Make these methods of a PeerConnection

(defn peer-node
  "Lookup a node by ID on a remote peer."
  [con id & [timeout]]
  (let [res-chan (request con 'node-by-uuid [id])]
    (if timeout
      (:result (lamina/wait-for-message res-chan timeout))
      res-chan)))

; TODO: Return a result channel and enqueue an error if we fail
(defn peer-query
  "Send a query to the given peer.  Returns a constant channel
  that will get the result of the query when it arrives."
  [con q & [timeout]]
  (let [res-chan (request con 'query [q])]
    (lamina/receive-all (lamina/fork res-chan)
                        #(log/to :peer "peer-query result: " %))
    (if timeout
      (:result (lamina/wait-for-message res-chan timeout))
      res-chan)))

(defn peer-sub-query
  [con q]
  (stream con 'sub-query [q]))

(defn peer-ping
  [con & [timeout]]
  (lamina/wait-for-message (request con 'ping nil) 
                           (or timeout 2000)))

(defmethod peer-sender "plasma"
  [url]
  (partial peer-sub-query (get-connection *manager* url)))

