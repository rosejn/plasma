(ns plasma.bootstrap
  (:use [plasma core connection peer util config network]
        jiraph.graph)
  (:require [plasma.query :as q]
            [lamina.core :as lamina]
            [logjam.core :as log]))

(defn- setup-graph
  [g]
  (with-graph g
    (clear-graph)
    (let [root (root-node)]
      (edge root (node) :label :net))))

(defn- peer-urls
  []
  (q/query (-> (q/path [peer [:net :peer]])
             (q/project 'peer :url))))

(defn- have-peer?
  [url]
  (contains? (set (peer-urls)) url))

(defn- net-root
  []
  (first (q/query (q/path [:net]))))

(defn- add-peer
  [id url]
  (log/to :bootstrap "add-peer: " id url)
  (edge (net-root) (proxy-node id url) :label :peer))

(defn- advertise-handler
  [peer con event]
  (let [[root-id url] (:params event)]
    (log/to :bootstrap "advertise-event:" url root-id)
    (with-graph (:graph peer)
                (unless (have-peer? url)
                        (add-peer root-id url)))))

(defn- bootstrap-connect-handler
  [p con]
  (lamina/receive-all (event-channel con :advertise)
                      (partial advertise-handler p con)))

(defn bootstrap-peer
  "Returns a peer that will automatically add new peers to its graph at
  [:net :peer] when they connect."
  ([path] (bootstrap-peer path {}))
  ([path options]
   (let [p (peer path options)]
     (setup-graph (:graph p))
     (on-connect p (partial bootstrap-connect-handler p))
     p)))

(def N-BOOTSTRAP-PEERS 5)

(defn add-bootstrap-peers
  [p con]
  (let [new-peers (peer-query con (-> (q/path [peer [:net :peer]])
                                  (q/choose N-BOOTSTRAP-PEERS)
                                  (q/project 'peer [:proxy :id]))
                              2000)]
    (with-graph (:graph p)
      (let [net (net-root)]
        (doseq [{url :proxy id :id} new-peers]
          (edge net (proxy-node id url) :label :peer))))))

(defn- advertise
  [con root-id url]
  (send-event con :advertise [root-id url]))

(defn bootstrap
  [peer boot-url]
  (let [con (get-connection (:manager peer) boot-url)
        root-id (with-graph (:graph peer) (root-node))
        my-url (public-url (:port peer))]
    (advertise con root-id my-url)
    (handle-peer-connection peer con)
    (add-bootstrap-peers peer con)))


