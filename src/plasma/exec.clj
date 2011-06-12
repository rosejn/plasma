(ns plasma.exec
  (:use
    [plasma util plan])
  (:require
    [logjam.core :as log]
    [lamina.core :as lamina]
    [jiraph.graph :as jiraph]))

(defn build-query
  "Iterate over the query operators, instantiating each one after its dependency
  operators have been instantiated."
  [plan timeout]
  (let [recv-chan (lamina/channel)
        sorted (op-dep-list plan)]
    (log/to :build "[build-query] sorted: " (seq sorted))
    (reduce
      (fn [ops op]
        (assoc ops (:id op)
               (op-node plan ops recv-chan timeout op)))
      {}
      sorted)))

(defn query-tree
  "Convert a query plan into a query execution tree."
  [plan timeout]
  {:type :query-tree
   :id (:id plan)
   :ops (build-query plan timeout)
   :root (:root plan)
   :params (:params plan)
   :timeout timeout})

; Query execution is initiated by loading parameters into param-op
; operator nodes.
(defn run-query
  "Execute a query by feeding parameters into a query operator tree."
  [tree param-map]
  (when-not jiraph/*graph*
    (throw (Exception. "Cannot run a query without binding a graph.
\nFor example:\n\t(with-graph G (query q))\n")))
  (log/format :flow "\n\n\n-----------------------------------------------------
[run-query]
query: %s
query-params: %s
input-params: %s"
              (trim-id (:id tree))
              (keys (:params tree))
              param-map)
  (doseq [[param-name param-id] (:params tree)]
    (let [param-val (if (contains? param-map param-name)
                      (get param-map param-name)
                      param-name)
          param-val (if (seq? param-val)
                      param-val
                      [param-val])
          param-op (get-in tree [:ops param-id])]
        (apply lamina/enqueue-and-close (get param-op :in) param-val)))
  #_(schedule (:timeout tree)
    (fn []
      (doseq [op (:ops tree)]
        (try
          (lamina/close (:out op))
          (catch Exception e))))))

(defn with-send-channel
  "Append channel to the end of each send operator's args list."
  [plan ch]
  (let [ops (map (fn [op]
                   (if (= :send (:type op))
                     (assoc op :args [ch])
                     op))
                 (vals (:ops plan)))
        ops (reduce (fn [mem op]
                      (assoc mem (:id op) op))
                    {}
                    ops)]
    (log/to :query "[with-send-channel] ops: " ops)
    (doseq [op (filter #(= :send (:type %)) (vals ops))]
      (log/to :query "[with-send-channel] op: " op))
    (assoc plan :ops ops)))

