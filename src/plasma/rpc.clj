(ns plasma.rpc
  (:use [plasma util])
  (:require [logjam.core :as log]))

(defn rpc-request
  "An RPC request to a remote service.  Passes params to method and returns
  the reply using the same id to correlate the response (or error) with the
  request."
  [id method params]
  (log/to :rpc "rpc-request[" (trim-id id) "]: " method params)
  {:type :request
   :id id
   :method method
   :params params})

(defn rpc-response
  "An RPC response matched to a request."
  [req val]
  (log/to :rpc "rpc-response[" (trim-id (:id req)) "]: "
          (if (seq? val)
            (take 5 (seq val))
            val))
  {:type :response
   :id (:id req)
   :result val})

(defn rpc-event
  "An RPC event is a one-shot message that doesn't expect a response."
  [id params]
  (log/to :rpc "rpc-event[" (if (uuid? id) (trim-id id) id) "]: " params)
  {:type :event
   :id id
   :params params})

(defn rpc-error
  "An RPC error for the given request."
  [req msg & [data]]
  (log/to :rpc "rpc-error[" (trim-id (:id req)) "]: " msg)
  {:type :error
   :id (:id req)
   :error {:message msg
           :data data}})

