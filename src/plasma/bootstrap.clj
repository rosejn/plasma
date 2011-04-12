(ns plasma.bootstrap
  (:use [plasma core connection peer util config network])
  (:require [plasma.query :as q]
            [lamina.core :as lamina]
            [logjam.core :as log]))

(log/file :bootstrap "boot.log")

(defn- setup-peer-graph
  [p]
  (with-peer-graph p
    (clear-graph)
    (let [root (root-node)]
      (make-edge root (make-node) :net))))

(defn- peer-urls
  [p]
  (with-peer-graph p
    (q/query (-> (q/path [peer [:net :peer]])
               (q/project 'peer :url)))))

(defn- have-peer?
  [p url]
  (contains? (set (peer-urls p)) url))

(defn- net-root
  [p]
  (with-peer-graph p
    (first (q/query (q/path [:net])))))

(defn- add-peer
  [p id url]
  (log/to :bootstrap "add-peer: " id url)
  (with-peer-graph p 
    (let [prx (make-proxy-node id url)
          net (net-root p)]
      (log/to :bootstrap "proxy: " prx "net: " net)
      (make-edge net prx :peer)
      (log/to :bootstrap "new-net: " (find-node net)
              "\nnew-proxy: " (find-node prx)))))

(defn- advertise-handler
  [p con event]
  (let [[root-id url] (:params event)]
    (log/to :bootstrap "advertise-event:" url root-id)
    (unless (have-peer? p url)
            (add-peer p root-id url))))

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
     (setup-peer-graph p)
     (on-connect p (partial bootstrap-connect-handler p))
     p)))

(def N-BOOTSTRAP-PEERS 5)
(def BOOTSTRAP-RETRY-PERIOD 500)

(defn add-bootstrap-peers
  [p con n]
  (let [new-peers (query con (-> (q/path [peer [:net :peer]])
                                    (q/project 'peer :proxy :id)
                                    (q/choose n)))]
    (log/to :bootstrap "n: " n "\nnew-peers: " (seq new-peers))
    (doseq [{url :proxy id :id} new-peers]
      (unless (get-node p id)
              (add-peer p id url))))
  (let [n-peers (first (query p (q/count*
                                  (q/path [:net :peer]))))]
    (log/to :bootstrap "n-peers: " n-peers)
    (when (< n-peers N-BOOTSTRAP-PEERS)
      (schedule BOOTSTRAP-RETRY-PERIOD
                #(add-bootstrap-peers p con (- N-BOOTSTRAP-PEERS n-peers))))))

(defn- advertise
  [con root-id url]
  (send-event con :advertise [root-id url]))

(defn bootstrap
  [p boot-url]
  (let [booter (peer-connection p boot-url)
        root-id (with-peer-graph p (root-node))
        my-url (public-url (:port p))]
    (advertise booter root-id my-url)
    (handle-peer-connection p booter)
    (add-bootstrap-peers p booter N-BOOTSTRAP-PEERS)))


