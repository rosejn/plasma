(ns plasma.route
  (:use [plasma core util connection peer digest]
        [clojure.contrib.math :only (expt)])
  (:require [logjam.core :as log]
            [lamina.core :as lamina]
            [plasma.query :as q]))

(defn flood-n
  [])

(defn random-walk-n
  [])

(defn greedy-iter
  [])

(defn addr-bits [host port n-bits]
  (mod
    (hex->int (hex (sha1 (str host port))))
    (expt 2 n-bits)))

(defn chord-distance [a b n-bits]
  (let [max-n (expt 2 n-bits)]
    (mod (+ (- b a) 
            max-n)
         max-n)))

(defn kademlia-distance [a b]
  (bit-xor a b))

(defn dht-lookup
  [p tgt-id]
  (let [res-chan (iter-n-query local 10 (q/path [:net :peer]))]
    (first (lamina/channel-seq res-chan 200))))

(defn closest-peer
  [p]
  (let [my-id (:guid (get-node p (root p)))
        peers (query (-> (q/path [p [:net :peer]])
                       (q/project 'p :id :guid :proxy)))]
    (take 1 (sort-by #(kademlia-distance my-id (:guid %)) peers))))


(defn dht-join 
  "Find peers with the addrs that fit in the slots of our peer table.
   The addrs closest to power of 2 distances from our ID

    guid + 2^0, 2^1, 2^2, 2^3, etc...
  "
  [p]
)
