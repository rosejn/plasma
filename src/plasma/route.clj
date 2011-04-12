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

(defn id-bits [id n-bits]
  (mod
    (hex->int (hex (sha1 id)))
    (expt 2 n-bits)))

(defn chord-distance [a b n-bits]
  (let [a (id-bits a n-bits)
        b (id-bits b n-bits)
        max-n (expt 2 n-bits)]
    (mod (+ (- b a) 
            max-n)
         max-n)))

(defn kademlia-distance [a b n-bits]
  (let [a (id-bits a n-bits)
        b (id-bits b n-bits)]
    (bit-xor a b)))

(defn closest-peer
  [p tgt-id]
  (let [peers (query p (-> (q/path [p [:net :peer]])
                         (q/project 'p :id :proxy)))]
    (take 1 (sort-by #(kademlia-distance tgt-id (:id %)) peers))))

(defn dht-lookup
  [p tgt-id]
  (let [root (get-node p ROOT-ID)]
    (loop [closest (assoc root
                          :con p)]
      (let [cp (closest-peer (:con closest) tgt-id)]
        (if (< (kademlia-distance tgt-id (:id cp))
               (kademlia-distance tgt-id (:id closest)))
          (recur (assoc cp :con (peer-connection p (:proxy cp))))
          closest)))))

(defn dht-join 
  "Find peers with the addrs that fit in the slots of our peer table.
   The addrs closest to power of 2 distances from our ID

    guid + 2^0, 2^1, 2^2, 2^3, etc...
  "
  [p]

)
