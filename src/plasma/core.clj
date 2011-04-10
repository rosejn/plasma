(ns plasma.core
  (:use
    [plasma util config]
    [lamina core]
    [aleph tcp formats]
    [jiraph graph]
    [clojure.contrib.core :only (dissoc-in)]))

; Special uuid used to query for a graph's root node.
(def ROOT-ID "UUID:ROOT")

(def *current-query* nil)
(def *current-binding* nil)
(def *current-path* nil)
(def *current-edge-preds* nil)
(def *proxy-results* nil)

(defmulti peer-sender
  (fn [url]
    (:proto (url-map url))))

(defn make-edge 
  "Create an edge from src to tgt with the associated properties.  At minimum
  there must be a :label property.
  
    (make-edge alice bob :label :friend)
  "
  [src tgt & {:as props}]
  {:pre [(contains? props :label)]}
  (update-node! :graph src #(assoc-in % [:edges tgt] props)))

(declare find-node)

(defn get-edges [node & [pred]]
  (let [n (find-node node)]
    (if pred
      (edges n pred)
      (edges n))))

(defn incoming-nodes
  "Return the set of source nodes for all incoming edges to the node with uuid."
  [uuid]
  (get-incoming :graph uuid))

(defn make-node
  "Create a node in the current graph that contains the given key-value pairs. If a UUID string is passed as the first argument then it will be used for the new node, otherwise a new one will be generated."
  [& key-vals]
  (let [[id key-vals] (if (and (odd? (count key-vals))
                               (string? (first key-vals)))
                        (list (first key-vals) (next key-vals))
                        (list (uuid) key-vals))]
    (apply add-node! :graph id :id id key-vals)
    id))

(defn remove-node
  "Remove a node and all of its incoming edges from the graph."
  [uuid]
  (let [incoming-sources (incoming-nodes uuid)]
    (doseq [src incoming-sources] ; remove incoming edges
      (update-node! :graph src #(dissoc-in % [:edges uuid])))
    (delete-node! :graph uuid)))

(defn remove-edge
  "Remove the edge going from src to tgt.  Optionally takes a predicate
  function that will be passed the property map for the edge, and the
  edge will only be removed if the predicate returns true."
  [src tgt & [pred]]
  (update-node! :graph src
    (fn [node]
      (if (fn? pred)
        (if (pred (get (:edges node) tgt))
          (dissoc-in node [:edges tgt])
          node)
        (dissoc-in node [:edges tgt])))))

(defn node-assoc
  "Associate key/value pairs with a given node id."
  [uuid & key-vals]
  (apply assoc-node! :graph uuid (apply hash-map key-vals)))

(defn root-node
  "Get the UUID of the root node of the graph."
  []
  (:root (meta *graph*)))

(defn find-node
  "Lookup a node map by UUID."
  [uuid]
  (unless *graph*
    (throw (Exception. "Cannot find-node without a bound graph.
For example:\n\t(with-graph G (find-node id))\n")))
  (let [uuid (if (= uuid ROOT-ID)
               (root-node)
               uuid)]
    (if-let [node (get-node :graph uuid)]
      (assoc node
             :id uuid
             :type :node))))

(defn- init-new-graph
  [root-id]
  (let [root (make-node root-id :label :root)
        meta (make-node (config :meta-id) :root root)]
    {:root root}))

(defn graph
  "Open a graph database located at path, creating it if it doesn't already exist."
  [path]
  (let [g {:graph (layer path)}]
    (with-graph g
      (let [meta (find-node (config :meta-id))
            meta (if meta
                   meta
                   (init-new-graph (uuid)))]
        (with-meta g meta)))))

(defn clear-graph
  []
  (truncate!)
  (init-new-graph (:root (meta *graph*))))

(defn proxy-node
  "Create a proxy node, representing a node on a remote graph which can be located by accessing the given url."
  [uuid url]
  (make-node uuid :proxy url))

(defn proxy-node?
  "Check whether the node with the given UUID is a proxy node."
  [uuid]
  (contains? (find-node uuid) :proxy))

(defmacro with-nodes! [bindings & body]
  (let [nodes (map (fn [[node-sym props]]
                     (let [props (seq (if (keyword? props)
                                   {:label props}
                                   props))]
                       [node-sym (apply concat '(make-node) props)]))
                   (partition 2 bindings))
        nodes (vec (apply concat nodes))]
    `(let ~nodes ~@body)))

(comment defn remove-edge [from to]
  (delete-edge! :graph from to))

