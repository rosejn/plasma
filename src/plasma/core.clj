(ns plasma.core
  (:use [aleph core tcp formats]))

(defn url-map [url]
  (let [match (re-find #"(.*)://([a-zA-Z-_.]*):([0-9]*)" url)
        [_ proto host port] match]
    {:proto proto
     :host host
     :port (Integer. port)}))

(defmulti peer-sender #(:proto (url-map %)))

