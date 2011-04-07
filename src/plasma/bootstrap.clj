(ns plasma.bootstrap
  (:use [plasma core connection peer util config]
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

(defn- bootstrap-connect-handler
  [p con]
  (future 
    (log/to :bootstrap "connect-handler from: " (:url con))
    (log/to :bootstrap "root: " (peer-node con ROOT-ID 100))
    (with-graph (:graph p)
                (unless (have-peer? (:url con))
                        (let [peer-root (peer-node con ROOT-ID 2000)]
                          (log/to :bootstrap "peer-root: " peer-root)
                          (add-peer (:id peer-root) (:url con)))))))

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
                              500)]
    (with-graph (:graph p)
      (let [net (net-root)]
        (doseq [{url :proxy id :id} new-peers]
          (edge net (proxy-node id url) :label :peer))))))

(defn bootstrap
  [p url]
  (let [con (get-connection (:manager p) url)]
    (handle-peer-connection p con)
    (add-bootstrap-peers p con)))

