(ns plasma.web
  (:use [ring.middleware file file-info]
        [lamina.core :exclude (restart)]
        [aleph formats http tcp]
        [plasma core config util connection peer]
        [clojure.contrib json]
        [clojure stacktrace])
  (:require [logjam.core :as log]))

; Remember: must be set in javascript client also.
(def WEB-PORT 4242)

(defn- rpc-handler
  [p req]
  (log/to :web "rpc-handler: " req)
  (let [res
        (case (:method req)
          "query"
          (with-peer-graph p
            (load-string
              (str
                "(require 'plasma.web)
                 (in-ns 'plasma.web)
                " (first (:params req))))))]
    {:result res
     :error nil
     :id (:id req)}))

(defn- request-handler
  [p ch msg]
  (when msg
    (log/to :web "\nMsg: " msg)
    (try
      (let [request (read-json msg true)
            _ (log/to :web "request: " request)
            res (rpc-handler p request)]
        (log/to :web "Result: " res)
        (enqueue ch (json-str res)))
      (catch Exception e
        (log/to :web "Request Exception: " e)
        (log/to :web "Trace: " (with-out-str (print-stack-trace e)))))))

(defn- dispatch-synchronous
  [request]
  (let [{:keys [request-method query-string uri]} request]
    (comment if (= uri "/")
      (home-view request)
      nil)))

(def sync-app
  (-> dispatch-synchronous
    (wrap-file "public")
    (wrap-file-info)))

(defn- server [p ch request]
  (log/to :web "client connect: " (str request))
  (if (:websocket request)
    (receive-all ch (partial request-handler p ch))
    (if-let [sync-response (sync-app request)]
      (enqueue ch sync-response)
      (enqueue ch {:status 404 :body "Page Not Found"}))))

(defn web-interface
  "Start a web interface for the give peer.
  To specify a custom port pass it in an options map:
    {:port 1234}
  "
  ([p] (web-interface p {:port WEB-PORT :websocket true}))
  ([p options]
   (start-http-server (partial server p) options)))

