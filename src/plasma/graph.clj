(ns plasma.graph
  (:use
    [plasma util config]
    [plasma.net url]
    [plasma.jiraph mem-layer]
    [clojure.contrib.core :only (dissoc-in)])
  (:require
    [logjam.core :as log]
    [jiraph.graph :as jiraph]
    [lamina.core :as lamina]))

; Special uuid used to query for a graph's root node.
(def ROOT-ID "UUID:ROOT")

(defonce node-events* (lamina/channel))
(defonce edge-events* (lamina/channel))
(defonce graph-events* (lamina/channel))

(lamina/siphon node-events* graph-events*)
(lamina/siphon edge-events* graph-events*)

(lamina/receive-all graph-events*
  (fn [event]
    (log/to :graph-event event)))

(defn node-event-channel
  [src-id]
  (lamina/filter*
    (fn [node-event]
      (= (:src-id node-event) src-id))
    node-events*))

(defn edge-event-channel
  ([id] (edge-event-channel id nil))
  ([id pred]
   (lamina/filter*
     (fn [{:keys [src-id tgt-id props]}]
       (and (= src-id id)
            (cond
              (keyword? pred) (= (:label props) pred)
              (fn? pred)      (pred props)
              (nil? pred)     true)))
     edge-events*)))

(defn- node-event
  [src-id new-props]
  (lamina/enqueue node-events*
    {:type :edge-event
     :src-id src-id
     :props new-props}))

(defn- edge-event
  [src-id tgt-id props]
  (lamina/enqueue edge-events*
    {:type :edge-event
     :src-id src-id
     :tgt-id tgt-id
     :props props}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Node functions
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn root-node-id
  "Get the UUID of the root node of the graph."
  []
  (:root (meta jiraph/*graph*)))

(defn make-node
  "Create a node in the current graph.  Returns the ID of the new node.

  (make-node)         ; create a node with a generated UUID and no properties
  (make-node \"foo\") ; create a node with ID foo and no properties
  (make-node {:a 123 :b \"asdf\"}) ; generates a UUID and saves props in node
  (make-node \"foo\" {:a 1324}) ; save node with id foo and props.
  "
  ([]
   (make-node (uuid) {}))
  ([arg]
   (if (map? arg)
     (if (:id arg)
       (make-node (:id arg) arg)
       (make-node (uuid) arg))
     (make-node arg {})))
  ([id props]
   (jiraph/add-node! :graph id :id id props)
   id))

(declare incoming-nodes)
(defn remove-node
  "Remove a node and all of its incoming edges from the graph."
  [uuid]
  (let [incoming-sources (incoming-nodes uuid)]
    (doseq [src incoming-sources] ; remove incoming edges
      (jiraph/update-node! :graph src #(dissoc-in % [:edges uuid])))
    (jiraph/delete-node! :graph uuid)))

(defn find-node
  "Lookup a node map by UUID."
  [uuid]
  (when-not jiraph/*graph*
    (throw (Exception. "Cannot find-node without a bound graph.
For example:\n\t(with-graph G (find-node id))\n")))
  (let [uuid (if (= uuid ROOT-ID)
               (root-node-id)
               uuid)]
    (if-let [node (jiraph/get-node :graph uuid)]
      (assoc node
             :id uuid
             :type :node))))

(defn assoc-node
  "Associate key/value pairs with a given node id."
  [uuid & key-vals]
  (let [uuid (if (= uuid ROOT-ID)
               (root-node-id)
               uuid)]
    (let [[old-props new-props] (apply jiraph/assoc-node! :graph
                                       uuid (apply hash-map key-vals))]
      (node-event uuid new-props))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Edge functions
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn make-edge
  "Create an edge from src to tgt with the associated properties.  At minimum
  there must be a :label property.

    (make-edge alice bob {:label :friend})
  "
  [src tgt label-or-props]
  (let [src (if (= ROOT-ID src)
              (root-node-id)
              src)
        tgt (if (= ROOT-ID tgt)
              (root-node-id)
              tgt)
        props (cond
                (keyword? label-or-props) {:label label-or-props}
                (and (map? label-or-props)
                     (contains? label-or-props :label)) label-or-props
                :default
                (throw (Exception. "make-edge requires either a keyword label or a map containing the :label property.")))]
    (jiraph/update-node! :graph src #(assoc-in % [:edges tgt] props))
    (edge-event src tgt props)))

(defn remove-edge
  "Remove the edge going from src to tgt.  Optionally takes a predicate
  function that will be passed the property map for the edge, and the
  edge will only be removed if the predicate returns true."
  [src tgt & [pred]]
  (jiraph/update-node! :graph src
    (fn [node]
      (if (fn? pred)
        (if (pred (get (:edges node) tgt))
          (dissoc-in node [:edges tgt])
          node)
        (dissoc-in node [:edges tgt])))))

(defn get-edges [node & [pred]]
  (let [n (find-node node)]
    (if pred
      (jiraph/edges n pred)
      (jiraph/edges n))))

(defn incoming-nodes
  "Return the set of source nodes for all incoming edges to the node with uuid."
  [uuid]
  (jiraph/get-incoming :graph uuid))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Graph functions
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- init-new-graph
  [root-id]
  (let [root (make-node root-id {:label :root})
        meta (make-node (config :meta-id) {:root root})]
    {:root root}))

(defn clear-graph
  []
  (jiraph/truncate!)
  (init-new-graph (:root (meta jiraph/*graph*))))

(defn open-graph
  "Open a graph database located at path, creating it if it doesn't already exist."
  [& [path]]
  (let [g (if path
            {:graph (jiraph/layer path)}
            {:graph (ref-layer)})]
    (jiraph/with-graph g
      (let [meta (find-node (config :meta-id))
            meta (if meta
                   meta
                   (init-new-graph (uuid)))]
        (with-meta g meta)))))

(defn make-proxy-node
  "Create a proxy node, representing a node on a remote graph which can be located by accessing the given url."
  [uuid url]
  (make-node uuid {:proxy url}))

(defn proxy-node?
  "Check whether the node with the given UUID is a proxy node."
  [uuid]
  (contains? (find-node uuid) :proxy))

(defmulti peer-sender
  (fn [url]
    (keyword (:proto (url-map url)))))

