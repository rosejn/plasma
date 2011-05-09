(ns plasma.net.presence
  (:import [java.net InetAddress])
  (:use [lamina core]
        [aleph udp]
        [plasma config]))

(def broadcasting? (atom false))
(def broadcaster*  (agent nil))

(defn presence-listener
  "Returns a channel that will receive all presence messages."
  []
  (let [msg-chan @(udp-object-socket {:port (config :presence-port)})]
    (filter* (fn [msg]
               (and (associative? msg)
                    (= :presence (:type msg))))
             msg-chan)))

(defn- presence-message
  "Create a presence message."
  []
  (let [{:keys [presence-ip presence-port plasma-version
                peer-id peer-port]} (config)
        msg {:type :presence
             :plasma-version plasma-version
             :peer-id peer-id
             :peer-port peer-port
             :peer-host (.getHostName (InetAddress/getLocalHost))}]
    {:msg msg :host presence-ip :port presence-port}))

(defn broadcast-presence
  "Start periodically broadcasting a presence message."
  []
  (send broadcaster* (fn [_] @(udp-object-socket)))
  (reset! broadcasting? true)
  (let [send-fn (fn sender [msg-chan]
                 (enqueue msg-chan (presence-message))
                 (Thread/sleep (config :presence-period))
                 (send-off broadcaster* sender))]
    (send-fn)))

