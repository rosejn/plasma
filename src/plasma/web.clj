(ns plasma.web
  (:use [ring.middleware file]
        [lamina.core :exclude (restart)]
        [aleph formats http tcp]
        [plasma core query]
        [jiraph graph])
  (:require [logjam.core :as log]))

(def GRAPH (graph "db/web"))

(defn request-handler 
  [ch]
  (fn [msg]
    (when msg
      (log/to :web "\nMsg: " msg)
      (try
        (let [res (with-graph GRAPH
                    (load-string 
                      (str 
                        "(require 'plasma.web)
                         (in-ns 'plasma.web)
                        " msg)))]
          (log/to :web "Result: " res)
          (enqueue ch (pr-str res)))
        (catch Exception e
          (log/to :web "Exception: " e))))))

(defn plasma-handler 
  [channel]
  (swap! clients conj channel)
  (receive-all channel (request-handler channel)))

(defn dispatch-synchronous
  [request]
  (let [{:keys [request-method query-string uri]} request]
    (comment if (= uri "/")
      (home-view request)
      nil)))

(def sync-app
  (-> dispatch-synchronous
    (wrap-file "public")))

(defn server [channel request]
  (log/to :web "app request: " (str request))
  (if (:websocket request)
    (plasma-handler channel)
    (if-let [sync-response (sync-app request)]
      (enqueue channel sync-response)
      (enqueue channel {:status 404 :body "Page Not Found"}))))

(defonce server* (atom identity))

(defn start []
  (reset! server* (start-http-server server {:port 4242 :websocket true})))

(defn stop []
  (@server*)
  (reset! server* identity))

(defn restart []
  (stop)
  (start))
