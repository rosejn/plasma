(ns plasma.connection-test
  (:use clojure.test
        [plasma config connection rpc])
  (:require [logjam.core :as log]
            [lamina.core :as lamina]))

(log/file :con "con.log")
(log/system :con)

(defrecord MockConnection
  [url]
  IConnection
  (request [c m p])
  (notify [c m p])
  (stream-channel [c m p])
  (request-channel [_])
  (notification-channel [_])
  (error-channel [_])

  IClosable
  (close [_]))

(deftest connection-cache-test
  (try
    (dotimes [i 4242]
      (refresh-connection (MockConnection. 
                            (str "tcp://plasma.org:" i)))
      (is (<= (count @connection-cache*)
              (config :connection-cache-limit))))
    (finally
      (clear-connection-cache))))

(deftest connection-rpc-test 
  (let [listener (connection-listener 1234)]
    (try
      (let [con-chan (connection-channel listener)
            req-chans (lamina/map* request-channel 
                       (lamina/filter* #(not (nil? %)) con-chan))]
        (lamina/receive-all req-chans
          (fn [chan]
            (lamina/receive-all chan
              (fn [[ch req]]
                (log/to :con "got: " req)
                (let [val (inc (first (:params req)))
                      res (rpc-response req val)]
                  (log/to :con "sending: " res)
                  (lamina/enqueue ch res))))))

        (let [client (connection "plasma://localhost:1234")
              res-chan (request client 'foo [10])
              res-msg (lamina/wait-for-message res-chan 1000)]
          (is (= 11 (:result res-msg)))
          (close client)))
      (finally
        (close listener)
        (clear-connection-cache)))))
