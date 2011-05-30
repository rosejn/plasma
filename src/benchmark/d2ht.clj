(ns benchmark.d2ht
  (:use plasma.core
        clojure.stacktrace
        [clojure.contrib.probabilities.random-numbers :only (rand-stream)])
  (:require [logjam.core :as log]
            [plasma.query.core :as q]
            [lamina.core :as lamina]
            [clojure.set :as set]
            [clojure.contrib.generic.collection :as gc]
            [clojure.contrib.probabilities.monte-carlo :as mc]
            [clojure.contrib.probabilities.finite-distributions :as dist]))

;D2HT:
;
;* a dht built using gossip
;* maintain neighbor sets for random and nearby peers
;* gossip with oldest peer in each set (reset age after each gossip)
; - delete if no response
;
;* one dimension (ring) for node ID
;* uses 2 sub protocols for maintaining the random and greedy net topologies

(def ADDR-BITS 8)

(def GOSSIP-PERIOD 2000)

(def KPS-PEERS 5)     ; parameter: L
(def KPS-ROUND-DROP 3) ; parameter: G

(def NPS-PEERS 15)     ; parameter: L

; Peers
; * proxy, timestamp, distance
; * each gossip round choose the least-fresh peer
; - if disconnected, remove and choose next peer

(defn d2ht-setup
  [p]
  (construct p
    (-> (nodes [net (q/path [:net])
                kps :kps
                nps :nps])
      (edges [net kps :kps
              net nps :nps]))))

;Kleinberg peer sampling: (KPS)
;* forms a small-world network
;* maintain L neighbors just for KPS
;* each round probabilistically keep the closest KPS neighbors, then remove and
;gossip the rest, G, of them
; - keep neighbors with the probability:
;
;      (/ 1.0 (distance a b))
;
;1) keep query:
;2) drop query: (inverse probability of keep query) 1 - p
;
;b) request their random peers
;  (-> (q/path [p [p-start :net :kps peer]])
;      (choose G)
;      (project ['p :id :proxy]))
;
;c) push peers
;  (-> (link (q/path [(select-gossip-peer-query) :net :kps :peer])
;                  (select-peers-to-drop) :peer)
;            :peer)

(defn with-peer-info
  [q]
  (-> q
    (q/project ['p :id :distance :timestamp :proxy])))

(defn kps-peers
  []
  (q/path [p [:net :kps :peer]]))

(defn add-kps-peer
  [p props]
  (log/to :d2 "adding: " (trim-id (:id props)))
  (let [dist (ring-abs-distance ADDR-BITS (peer-id p) (:id props))
        ts   (current-time)
        props (assoc props :timestamp ts :distance dist)]
    (construct p
      (-> (nodes [kps (q/path [:net :kps])
                  new-peer props])
        (edges [kps new-peer :peer])))))

(defn add-kps-peers
  [p peers]
  (doseq [new-peer peers]
    (add-kps-peer p new-peer)))

(defn oldest-kps-peer
  "Select the least-fresh peer."
  [p]
  (first (query p (with-peer-info
             (-> (kps-peers)
               (q/order-by 'p :timestamp :asc)
               (q/limit 1))))))

(defn- take-sample
  [n rt]
  (take n (gc/seq (mc/random-stream rt rand-stream))))

(defn harmonic-close-peers
  "Select n peers with a probability proportional to 1/distance.
  (harmonic probability function)"
  [peers n]
  (if (<= (count peers) n)
    peers
    (let [dist-map (reduce (fn [m v] (assoc m (:id v) (/ 1.0 (:distance v)))) {} peers)
          peer-dist (mc/discrete (dist/normalize dist-map))
          keepers (loop [to-keep #{}]
                    (let [tkc (count to-keep)]
                      (if (= n tkc)
                        to-keep
                        (recur (set/union to-keep
                                          (set (take-sample (- n tkc)
                                                            peer-dist)))))))]
      (filter #(keepers (:id %)) peers))))

;(let [test-peers (map (fn [v] {:id v :dist (* 100 v)}) (range 10))]
;  (harmonic-close-peers test-peers 3))

(defn remove-peers
  [p ptype peers]
  (let [type-id (:id (query p (q/path [:net ptype])))]
    (with-peer-graph p
      (doseq [p-id (map :id peers)]
        (remove-edge type-id p-id)))))

(defn kps-exchange-peers
  [p]
  (let [peers (query p (with-peer-info (kps-peers)))
        keepers (harmonic-close-peers peers (- KPS-PEERS KPS-ROUND-DROP))
        to-exchange (filter #((set (map :id keepers)) (:id %)) peers)
        to-exchange (conj to-exchange {:id (peer-id p) :proxy (:url p)})]
    (remove-peers p :kps to-exchange)
    (log/to :d2 "exchanging: " (vec (map (comp trim-id :id) to-exchange)))
    to-exchange))

(defn update-timestamp
  [p id]
  (with-peer-graph p (assoc-node id :timestamp (current-time))))

(defn kps-gossip
  "Perform a single round of kps gossiping."
  [p]
  (when-let [url (:proxy (oldest-kps-peer p))]
    (log/to :d2 "gossip url: " url)
    (let [partner (peer-connection p url)
          to-exchange (kps-exchange-peers p)]
      (let [res-chan (request partner 'kps-exchange [to-exchange])]
        (lamina/receive res-chan
          (fn [res]
            (log/to :d2 "response: " res)
            (add-kps-peers p (:result res)))))
      (update-timestamp p (:id partner)))))

(defmethod rpc-handler 'kps-exchange
  [p req]
  (let [new-peers (first (:params req))
        to-exchange (kps-exchange-peers p)]
    (add-kps-peers p new-peers)
    to-exchange))

(defn kps-on
  [p period]
  (assoc p :kps-timer (periodically period (partial kps-gossip p))))

(defn kps-off
  [p]
  (if-let [t (:kps-timer p)]
    (t))
  (dissoc p :kps-timer))

;Neighbor peer sampling: (NPS)
;* gossip request to the oldest peers (either NPS or KPS) who are also closest to
;you in distance, and request their neighbors closest to you

(defn all-peers
  []
  (q/path [p [:net #"nps|kps" :peer]]))

(defn oldest-n-peers
  [p n]
  (query p
         (-> (all-peers)
           (q/order-by 'p :distance :asc)
           (q/limit n))))

(defn closest-n-peers-to-q
  "Return the closest n peers to neighbor q."
  [p q n]
  (take n (sort-by
            (fn [b] (ring-abs-distance (:id q) (:id b)))
            (query p (all-peers)))))

(defn nps-peers
  []
  (q/path [p [:net :nps :peer]]))

(defn add-nps-peer
  [p props]
  (let [dist (ring-abs-distance (peer-id p) (:id props))
        ts   (current-time)
        props (assoc props :timestamp ts :distance dist)]
    (construct p
      (-> (nodes [nps (q/path [:net :nps])
                  new-peer props])
        (edges [kps new-peer :peer])))))

(defn add-nps-peers
  [p new-peers]
  (let [cur-peers (query p (with-peer-info (nps-peers)))
        closest-peers (take NPS-PEERS
                            (sort-by
                              (fn [b] (ring-abs-distance (:id p) (:id b)))
                              (concat cur-peers new-peers)))]
  (doseq [new-peer closest-peers :when (not (get-node p (:id new-peer)))]
    (add-nps-peer p new-peer))))

(defn nps-exchange-peers
  [p q]
  (-> (closest-n-peers-to-q p q 5)
    (conj {:id (peer-id p) :proxy (:url p)})))

(defn nps-gossip
  "Perform a round of NPS gossiping."
  [p]
  (let [oldies (oldest-n-peers p 4)
        q-peer (first (harmonic-close-peers oldies 1))
        q-con (peer-connection p (:proxy q-peer))
        to-exchange (nps-exchange-peers p q-peer)
        res-chan (request q-con 'nps-exchange [to-exchange])]
      (lamina/receive res-chan
        #(add-nps-peers p %))
    (update-timestamp p (:id q-peer))))

(defmethod rpc-handler 'nps-exchange
  [p req]
  (let [new-peers (first (:params req))
        to-exchange (nps-exchange-peers p)]
    (add-nps-peers new-peers)
    to-exchange))

(defn kps-on
  [p period]
  (assoc p :kps-timer (periodically period (partial kps-gossip p))))

(defn kps-off
  [p]
  (if-let [t (:kps-timer p)]
    (t))
  (dissoc p :kps-timer))

#_(defn start
  [n start-delay n-searches]
  (let [s-port 23423
        s-url (plasma-url "localhost" s-port)
        strapper (bootstrap-peer {:port s-port})
        peers (doall (take n (map #(peer {:port %})
                                  (iterate inc (inc s-port)))))]
    (doseq [p peers]
      (d2ht-setup p))
    [strapper peers]))

(defn addp
  "Add peer b to peer a's kps view."
  [a b]
  (add-kps-peer a {:id (peer-id b) :proxy (plasma-url "localhost" (:port b))}))


(defn setup
  []
  (log/file :d2 "d2.log")
  (def peers (doall (take 9 (map #(peer {:port %})
                                 (iterate inc (inc 1234))))))
  (doseq [p peers] (d2ht-setup p))
  (def p1 (nth peers 0))
  (def p2 (nth peers 1))
  (doseq [p (drop 1 peers)] (addp p p1)))

(defn gossip
  []
  (doseq [p peers] (kps-gossip p)))

(defn pc
  []
  (println "peer-counts: " (vec (map #(count (query % (all-peers))) peers))))

(defn ids
  []
  (println "peer-ids: " (vec (map #(trim-id (peer-id %)) peers))))

