(ns plasma.peer-test
  (:use [plasma core peer query]
        [lamina core]
        [jiraph graph]
        test-utils
        clojure.test
        clojure.stacktrace)
  (:require [logjam.core :as log]))

(log/file :peer "peer.log")

(deftest peer-pool-test []
  (dotimes [i 1000]
    (refresh-peer {:host "test.com"
                         :port i})
    (is (<= (count @peer-pool*)
           MAX-POOL-SIZE))))

(defn- init-peer
  [p]
  (with-graph (:graph p)
    (clear-graph)
    (test-graph)))

(deftest peer-query-test []
  (let [port (+ 1000 (rand-int 10000))
        p1 (local-peer "db/p1" port)]
    (try
      (init-peer p1)
      (let [con (peer "localhost" port)]
        (is (= :pong (wait-for-message (peer-query con :ping) 1000)))
        (is (= "self" (:label (wait-for-message (peer-query con ROOT-ID) 1000))))
        (let [res (wait-for-message (peer-query con (path [synth [:music :synths :synth]]
                                                            (where (>= (:score synth) 0.6))
                                                            synth)) 1000)]
          (is (= 2 (count res)))
          (is (= #{:bass :kick} (set (map :label
                                          (map #(wait-for-message (peer-query con %))
                                               res)))))))
      (finally
        (peer-close p1)))))

(deftest proxy-node-test []
  (let [port (+ 1000 (rand-int 10000))
        p1 (local-peer "db/p1" port)]
    (try
      (init-peer p1)
      (let [con (peer "localhost" port)]
        (is (= :pong (wait-for-message (peer-query con :ping) 1000)))
        (is (= "self" (:label (wait-for-message (peer-query con ROOT-ID) 1000))))
        (let [res (wait-for-message (peer-query con (path [synth [:music :synths :synth]]
                                                            (where (>= (:score synth) 0.6))
                                                            synth)) 1000)]
          (is (= 2 (count res)))
          (is (= #{:bass :kick} (set (map :label
                                          (map #(wait-for-message (peer-query con %))
                                               res)))))))
      (finally
        (peer-close p1)))))

