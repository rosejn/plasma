(ns plasma.peer-test
  (:use plasma.peer
        [aleph core]
        :reload-all)
  (:use clojure.test
        clojure.stacktrace))

(deftest connection-pool-test []
  (dotimes [i 1000]
    (refresh-connection {:host "test.com"
                         :port i})
    (is (<= (count @connection-pool*)
           MAX-POOL-SIZE))))

(def port (+ 1000 (rand-int 10000)))
(def P1 (peer "db/p1" port))

(deftest peer-test []
  (let [con (peer-connection "localhost" port)
        resp (remote-query con "ping")]
    (is (= "pong" (wait-for-message resp 1000)))))
