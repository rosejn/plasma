(ns plasma.peer-test
  (:use [plasma core peer query] :reload-all
        [lamina core]
        [jiraph graph]
        test-utils
        clojure.test
        clojure.stacktrace)
  (:require [logjam.core :as log]))

;(log/file :peer "peer.log")
;(log/console :peer)

(deftest peer-pool-test []
  (try
    (dotimes [i 1000]
      (refresh-peer {:host "test.com"
                     :port i
                     :connection (channel)}) ;(fn [_] nil)})
      (is (<= (count @peer-pool*)
              MAX-POOL-SIZE)))
    (finally
      (clear-peer-pool))))

(defn- reset-peer
  [p]
  (with-graph (:graph p)
    (clear-graph)
    (test-graph)))

(deftest peer-send-test []
  (let [port (+ 1000 (rand-int 10000))
        local (local-peer "db/p1" port)]
    (try
      (reset-peer local)
      (let [p (peer "localhost" port)]
        (is (= :pong (peer-query p :ping 1000)))
        (is (uuid? (peer-query p ROOT-ID 1000)))
        (let [q (path [synth [:music :synths :synth]]
                  (where (>= (:score synth) 0.6)))
              res (peer-query p q 1000)]
          (is (= 2 (count res)))
          (is (= #{:bass :kick} (set (map :label
                                          (map #(wait-for-message (peer-query p %))
                                               res)))))))
      (finally
        (peer-close local)
        (clear-peer-pool)))))

(comment deftest proxy-node-test []
  (let [port (+ 1000 (rand-int 10000))
        local (local-peer "db/p1" port)
        remote (local-peer "db/p2" (inc port))]
    (try
      (reset-peer local)
      (reset-peer remote)
      (let [local-root (peer-query local (root-node) 200)
            net (first (query (path [:net])))
            peer-node (proxy-node local-root (str "plasma://localhost:" (inc port)))
            link (edge net peer-node :label :peer)]
        (let [res (peer-query p (path [:net :peer :music :synths :synth]) 1000)]
          (is (= #{:kick :bass :snare :hat}
           (set (map #(:label (find-node %))
                     (query (path [:music :synths :synth]))))))))
      (finally
        (peer-close local)
        (peer-close remote)
        (clear-peer-pool)))))

