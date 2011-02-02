(ns plasma.peer-test
  (:use [plasma core peer query] :reload-all
        [lamina core]
        [jiraph graph]
        test-utils
        clojure.test
        clojure.stacktrace)
  (:require [logjam.core :as log]))

;(log/file :peer "peer.log")
(log/console :peer)
;(log/console :op)

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

(deftest peer-send-test []
  (let [port (+ 1000 (rand-int 10000))
        local (local-peer "db/p1" port)]
    (try
      (init-peer local)
      (let [p (peer "localhost" port)]
        (is (= :pong (wait-for-message (peer-query p :ping) 1000)))
        (is (= "self" (:label (wait-for-message (peer-query p ROOT-ID) 1000))))
        (let [res (wait-for-message (peer-query p (path [synth [:music :synths :synth]]
                                                            (where (>= (:score synth) 0.6))
                                                            synth)) 1000)]
          (is (= 2 (count res)))
          (is (= #{:bass :kick} (set (map :label
                                          (map #(wait-for-message (peer-query p %))
                                               res)))))))
      (finally
        (peer-close local)
        (clear-peer-pool)))))

(deftest proxy-node-test []
  (let [port (+ 1000 (rand-int 10000))
        local (local-peer "db/p1" port)]
    (try
      (init-peer local)
      (let [p (peer "localhost" port)]
        (is (= :pong (wait-for-message (peer-query p :ping) 1000)))
        (is (= "self" (:label (wait-for-message (peer-query p ROOT-ID) 1000))))
        (let [res (wait-for-message (peer-query p (path [synth [:music :synths :synth]]
                                                            (where (>= (:score synth) 0.6))
                                                            synth)) 1000)]
          (is (= 2 (count res)))
          (is (= #{:bass :kick} (set (map :label
                                          (map #(wait-for-message (peer-query p %))
                                               res)))))))
      (finally
        (peer-close local)
        (clear-peer-pool)))))

