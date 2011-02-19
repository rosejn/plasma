(ns plasma.core
  (:use
    [lamina core]
    [aleph tcp formats]
    [jiraph graph]))

(def META-ID "UUID:META")

(def DB-PATH "db")
(def *current-query* nil)
(def *current-binding* nil)
(def *current-path* nil)
(def *current-edge-preds* nil)
(def *proxy-results* nil)

(defn url-map [url]
  (let [match (re-find #"(.*)://([a-zA-Z-_.]*):([0-9]*)" url)
        [_ proto host port] match]
    {:proto proto
     :host host
     :port (Integer. port)}))

(defmulti peer-sender #(:proto (url-map %)))

(comment defn plasma-graph
  "You need to call this function with the path to your local plasma database
  before using the rest of this library.  If no DB currently exists, a new
  one will be created."
  [g]
  (open! g)
  (set-graph! g))

(defn clear-graph []
  (truncate!))

(defn uuid
  "Creates a random, immutable UUID object that is comparable using the '=' function."
  [] (str "UUID:" (. java.util.UUID randomUUID)))

(defn uuid? [s]
  (and (string? s)
       (= (seq "UUID:") (take 5 (seq s)))))

(defn node 
  "Create a node in the current graph that contains the given key-value pairs. If a UUID string is passed as the first argument then it will be used for the new node, otherwise a new one will be generated."
  [& key-vals]
  (let [[id key-vals] (if (and (odd? (count key-vals))
                               (string? (first key-vals)))
                        (list (first key-vals) (next key-vals))
                        (list (uuid) key-vals))]
    (apply add-node! :graph id :id id key-vals)
    id))

(defn find-node 
  "Lookup a node map by UUID."
  [uuid]
  (get-node :graph uuid))

(defn- init-new-graph
  []
  (let [root (node :label :root)
        meta (node META-ID :root root)]
    {:root root}))

(defn graph
  "Open a graph database located at path, creating it if it doesn't already exist."
  [path]
  (let [g {:graph (layer path)}]
    (with-graph g
      (let [meta (find-node META-ID)
            meta (if meta 
                   meta 
                   (init-new-graph))]
        (with-meta g meta)))))

(defn root-node
  "Get the UUID of the root node of the graph."
  []
  (:root (meta *graph*)))

(defn proxy-node 
  "Create a proxy node, representing a node on a remote graph which can be located by accessing the given url."
  [uuid url]
  (node uuid :proxy url))

(defn proxy-node? 
  "Check whether the node with the given UUID is a proxy node."
  [uuid]
  (contains? (find-node uuid) :proxy))

(defmacro with-nodes! [bindings & body]
  (let [nodes (map (fn [[node-sym props]]
                     (let [props (seq (if (keyword? props)
                                   {:label props}
                                   props))]
                       [node-sym (apply concat '(node) props)]))
                   (partition 2 bindings))
        nodes (vec (apply concat nodes))]
    `(let ~nodes ~@body)))

(defn edge [from to & {:as props}]
  {:pre [(contains? props :label)]}
  (append-node! :graph from
    {:edges {to props}}))

(defn get-edges [node & [pred]]
  (let [n (find-node node)]
    (if pred
      (edges n pred)
      (edges n))))

(comment defn remove-edge [from to]
  (delete-edge! :graph from to))

