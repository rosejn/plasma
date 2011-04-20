(ns plasma.url
  (:use [plasma config]))

(defn url
  ([proto host]
   (str proto "://" host))
  ([proto host port]
   (str proto "://" host ":" port)))

(defn plasma-url
  [host port]
  (url (config :protocol) host port))

(defn url-map [url]
  (let [match (re-find #"(.*)://([a-zA-Z-_.]*):([0-9]*)" url)
        [_ proto host port] match]
    {:proto proto
     :host host
     :port (Integer. port)}))

