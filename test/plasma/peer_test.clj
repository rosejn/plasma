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
        (is (uuid?   (peer-query p ROOT-ID 1000)))
        (let [q (path [synth [:music :synths :synth]]
                  (where (>= (:score synth) 0.6)))
              res (peer-query p q 2000)]
          (is (= 2 (count res)))
          (is (= #{:bass :kick} (set (map :label
                                          (map #(wait-for-message (peer-query p %))
                                               res)))))))
      (finally
        (peer-close local)
        (clear-peer-pool)))))

(deftest proxy-node-test []
  (let [port (+ 1000 (rand-int 10000))
        local (local-peer "db/p1" port)
        remote (local-peer "db/p2" (inc port))
        local-p (peer "localhost" port)
        remote-p (peer "localhost" (inc port))]
    (try
      (reset-peer local)
      (reset-peer remote)
      (let [remote-root (peer-query remote-p ROOT-ID 2000)
            net (first (peer-query local-p (path [:net]) 2000))
            peer-node (with-graph (:graph local)
                        (proxy-node remote-root
                                    (str "plasma://localhost:" (inc port))))
            link (with-graph (:graph local)
                   (edge net peer-node :label :peer))]
        (let [q (-> (path [synth [:net :peer :music :synths :synth]])
                  (project 'synth :label))
              res (peer-query local-p q 2000)]
          (is (= #{:kick :bass :snare :hat}
                 (set (map :label res))))
          (println "res: " res)))
      (finally
        (peer-close local)
        (peer-close remote)
        (clear-peer-pool)))))

(deftest many-proxy-node-test []
  (let [n-peers 10
        port (+ 1000 (rand-int 10000))
        local (local-peer "db/local" port)
        local-p (peer "localhost" port)
        peers (doall 
                (map
                  (fn [n]
                    (let [p (local-peer (str "db/peer-" n) (+ port n 1))]
                      (with-graph (:graph p)
                        (clear-graph)
                        (let [root-id (root-node)]
                          (node-assoc root-id :peer-id n)
                          (with-nodes! [net :net
                                        docs :docs
                                        a {:label (str "a-" n) :score 0.1}
                                        b {:label (str "b-" n) :score 0.5}
                                        c {:label (str "c-" n) :score 0.9}]
                            (edge root-id net :label :net)
                            (edge root-id docs :label :docs)
                            (edge docs a :label :doc)
                            (edge docs b :label :doc)
                            (edge docs c :label :doc))
                          [p root-id n]))))
                  (range n-peers)))]
    (try
      (with-graph (:graph local)
        (clear-graph)
        (let [root-id (root-node)
              net (node :label :net)]
          (edge root-id net :label :net)
          (doseq [[p peer-root n] peers]
            (edge net 
                  (proxy-node peer-root (str "plasma://localhost:" (+ port n 1)))
                  :label :peer))
         ; (println "peers: " (query (path [:net :peer])))
          ))
        (let [q (-> (path [doc [:net :peer :docs :doc]]
                      (where (> (:score doc) 0.5)))
                  (project 'doc :label :score))
              res (peer-query local-p q 4000)]
          (println "res: " res)
          (is (= (* 3 n-peers) (count res))))
      (finally
        ; Ghetto stuff...
        ; Until Clojure supports recur in a try/finally form
        (peer-close local) 
        (peer-close (first (nth peers 0)))
        (peer-close (first (nth peers 1)))
        (peer-close (first (nth peers 2)))
        (peer-close (first (nth peers 3)))
        (peer-close (first (nth peers 4)))
        (peer-close (first (nth peers 5)))
        (peer-close (first (nth peers 6)))
        (peer-close (first (nth peers 7)))
        (peer-close (first (nth peers 8)))
        (peer-close (first (nth peers 9)))
        ;(doseq [[p root-id n] peers]
        ;  (peer-close p))
        (clear-peer-pool)))))
