(ns plasma.web
  (:use [ring.middleware file file-info]
        [lamina.core :exclude (restart)]
        [aleph formats http tcp]
        [plasma core query]
        [jiraph graph]
        [clojure.contrib json]
        [clojure stacktrace])
  (:require [logjam.core :as log]))

(defonce GRAPH (graph "db/web"))
(def DEFAULT-PORT 4242) ; Remember: must be set in javascript client also.

(defn rpc-handler
  [req]
  (log/to :web "rpc-handler: " req)
  (let [res 
        (case (:method req)
          "query"
          (with-graph GRAPH
                      (load-string
                        (str
                          "(require 'plasma.web)
                          (in-ns 'plasma.web)
                          " (first (:params req))))))]
    {:result res
     :error nil
     :id (:id req)}))

(defn request-handler
  [ch msg]
  (when msg
    (log/to :web "\nMsg: " msg)
    (try
      (let [request (read-json msg true)
            _ (log/to :web "request: " request)
            res (rpc-handler request)]
        (log/to :web "Result: " res)
        (enqueue ch (json-str res)))
      (catch Exception e
        (log/to :web "Request Exception: " e)
        (log/to :web "Trace: " (with-out-str (print-stack-trace e)))))))

(defn dispatch-synchronous
  [request]
  (let [{:keys [request-method query-string uri]} request]
    (comment if (= uri "/")
      (home-view request)
      nil)))

(def sync-app
  (-> dispatch-synchronous
    (wrap-file "public")
    (wrap-file-info)))

(defn server [ch request]
  (log/to :web "client connect: " (str request))
  (if (:websocket request)
    (receive-all ch 
                 (fn [msg]
                   (request-handler ch msg)))
    (if-let [sync-response (sync-app request)]
      (enqueue ch sync-response)
      (enqueue ch {:status 404 :body "Page Not Found"}))))

(defonce server* (atom #()))

(defn start []
  (reset! server* (start-http-server server {:port 4242 :websocket true})))

(defn stop []
  (@server*)
  (reset! server* #()))

(defn restart []
  (stop)
  (start))
