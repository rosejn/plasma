(ns plasma.remote)

(defn- url-map [url]
  (let [match (re-find #"(.*)://([0-9a-zA-Z-_.]*)(:([0-9]+))?(.*)" url)
        [_ proto host _ port path] match]
    {:proto proto
     :host host
     :port (if port
             (Integer. port)
             nil)
     :path path}))

(defmulti remote-query-fn
  "Given a URL, return a function that accepts a query plan and returns a query channel
  onto which the results will be enqueued.  This is the mechanism used to traverse
  from one graph to another, possibly over the network.  Dispatch is based on the
  protocol as a keyword (e.g. :http, :plasma, :jiraph, etc...)"
  (fn [url]
    (keyword (:proto (url-map url)))))
