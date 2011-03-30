(ns plasma.connection-test
  (:use clojure.test
        [plasma config util connection rpc])
  (:require [logjam.core :as log]
            [lamina.core :as lamina]))

(log/file :con "con.log")
(log/repl :con)

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
  (let [manager (connection-manager)]
    (try
      (dotimes [i 300]
        (refresh-connection manager (MockConnection.
                                    (str "tcp://plasma.org:" i)))
        (is (<= (connection-count manager)
                (config :connection-cache-limit))))
      (finally
        (clear-connections manager)))))

(deftest connection-rpc-test
  (let [manager (connection-manager)
        listener (connection-listener manager 1234)]
    (try
      (on-connect listener
        (fn [con]
          (let [requests (request-channel con)]
            (lamina/receive-all requests
              (fn [[ch req]]
                (let [val (* 2 (first (:params req)))
                      res (rpc-response req val)]
                  (lamina/enqueue ch res)))))))

        (let [client (get-connection manager (plasma-url "localhost" 1234))]
          (dotimes [i 20]
            (let [res-chan (request client 'foo [i])
                  res (lamina/wait-for-message res-chan 100)]
              (is (= (* 2 i) (:result res)))))
          (close client))
      (finally
        (close listener)
        (clear-connections manager)))))

(deftest connection-notification-test
  (let [manager (connection-manager)
        listener (connection-listener manager 1234)]
    (try
      (let [events (atom [])]
        (on-connect listener
          (fn [con]
            (lamina/receive-all (notification-channel con)
              (fn [event]
                (swap! events conj event)))))
        (let [client (get-connection manager (plasma-url "localhost" 1234))]
          (dotimes [i 20]
            (notify client 'foo [:a :b :c]))
          (close client))
        (Thread/sleep 100)
        (is (= 20 (count @events))))
      (finally
        (close listener)
        (clear-connections manager)))))

(deftest connection-stream-test
  (let [manager (connection-manager)
        listener (connection-listener manager 1234)]
    (try
      (on-connect listener
        (fn [con]
          (lamina/receive-all (stream-channel con)
            (fn [[s-chan msg]]
              (lamina/enqueue s-chan (inc (first (:params msg))))
              (lamina/receive-all s-chan
                                  (fn [v]
                                    (lamina/enqueue s-chan (inc v))))))))

        (let [client (get-connection manager (plasma-url "localhost" 1234))
              s-chan (stream client 'foo [1])
              res (atom nil)]
          (lamina/receive s-chan #(lamina/enqueue s-chan (inc %)))
          (Thread/sleep 100)
          (lamina/receive s-chan #(lamina/enqueue s-chan (inc %)))
          (Thread/sleep 100)
          (lamina/receive s-chan #(reset! res %))
          (Thread/sleep 100)
          (is (= 6 @res))
          (close client))
      (finally
        (close listener)
        (clear-connections manager)))))

