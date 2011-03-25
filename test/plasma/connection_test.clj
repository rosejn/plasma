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
  (request-channel [_])
  (notify [c m p])
  (notification-channel [_])
  (stream [c m p])
  (stream-channel [c])

  IClosable
  (close [_]))

(deftest connection-cache-test
  (try
    (dotimes [i 300]
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
                (let [val (* 2 (first (:params req)))
                      res (rpc-response req val)]
                  (lamina/enqueue ch res))))))

        (let [client (connection "plasma://localhost:1234")]
          (dotimes [i 20]
            (let [res-chan (request client 'foo [i])
                  res (lamina/wait-for-message res-chan 100)]
              (is (= (* 2 i) (:result res)))))
          (close client)))
      (finally
        (close listener)
        (clear-connection-cache)))))

(deftest connection-notification-test 
  (let [listener (connection-listener 1234)]
    (try
      (let [events (atom [])
            con-chan (connection-channel listener)
            notify-chans (lamina/map* notification-channel 
                       (lamina/filter* #(not (nil? %)) con-chan))]
        (lamina/receive-all notify-chans
          (fn [chan]
            (lamina/receive-all chan
              (fn [event]
                (swap! events conj event)))))

        (let [client (connection "plasma://localhost:1234")]
          (dotimes [i 20]
            (notify client 'foo [:a :b :c]))
          (close client))
        (Thread/sleep 100)
        (is (= 20 (count @events))))
      (finally
        (close listener)
        (clear-connection-cache)))))

(deftest connection-stream-test
  (let [listener (connection-listener 1234)]
    (try
      (let [con-chan (connection-channel listener)
            stream-chans (lamina/map* stream-channel 
                                      (lamina/filter* #(not (nil? %)) con-chan))]
        (lamina/receive-all stream-chans
          (fn [chan]
            (lamina/receive-all chan
              (fn [[s-chan msg]]
                (lamina/enqueue s-chan (inc (first (:params msg))))
                (lamina/receive-all s-chan 
                  (fn [v]
                    (lamina/enqueue s-chan (inc v))))))))

        (let [client (connection "plasma://localhost:1234")
              s-chan (stream client 'foo [1])
              res (atom nil)]
          (lamina/receive s-chan #(lamina/enqueue s-chan (inc %)))
          (Thread/sleep 100)
          (lamina/receive s-chan #(lamina/enqueue s-chan (inc %)))
          (Thread/sleep 100)
          (lamina/receive s-chan #(reset! res %))
          (Thread/sleep 100)
          (is (= 6 @res))
          (close client)))
      (finally
        (close listener)
        (clear-connection-cache)))))

