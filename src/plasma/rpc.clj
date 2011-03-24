(ns plasma.rpc
  (:require [logjam.core :as log]))

; A simple RPC protocol, initially based on JSON-RPC

(defn rpc-request
  "An RPC request to a remote service.  Passes params to method and returns
  the reply using the same id to correlate the response (or error) with the
  request."
  [id method params]
  {:id id
   :method method
   :params params})

(defn rpc-response
  "An RPC response matched to a request."
  [req val]
  {:id (:id req)
   :result val})

(defn rpc-notify
  "An RPC notification is a method call that doesn't expect a response."
  [method params]
  {:method method
   :params params})

(defn rpc-error
  "An RPC error for the given request."
  [req msg & [data]]
  {:id (:id req)
   :error {:message msg
           :data data}})

