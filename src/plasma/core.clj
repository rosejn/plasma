(ns plasma.core
  (:use [aleph core tcp formats]
        jiraph))

(def ROOT-ID "ROOT")

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

(defn plasma-graph
  "You need to call this function with the path to your local plasma database
  before using the rest of this library.  If no DB currently exists, a new
  one will be created."
  [g]
  (open-graph g)
  (set-graph! g))

(defn clear-graph []
  (truncate-graph!))

(defn uuid
  "Creates a random, immutable UUID object that is comparable using the '=' function."
  [] (str "UUID:" (. java.util.UUID randomUUID)))

(defn uuid? [s]
  (and (string? s)
       (= (seq "UUID:") (take 5 (seq s)))))

(defn node [& key-vals]
  (let [[id key-vals] (if (and (odd? (count key-vals))
                               (string? (first key-vals)))
                        (list (first key-vals) (next key-vals))
                        (list (uuid) key-vals))]
    (apply add-node! :graph id key-vals)
    id))

(defn find-node [id]
  (get-node :graph id))

(defn proxy-node [url]
  (node :proxy url))

(defn proxy-node? [id]
  (contains? (find-node id) :proxy))

(defmacro with-nodes! [bindings & body]
  (let [nodes (map (fn [[node-sym props]]
                     (let [props (seq (if (keyword? props)
                                   {:label props}
                                   props))]
                       [node-sym (apply concat '(node) props)]))
                   (partition 2 bindings))
        nodes (vec (apply concat nodes))]
    `(let ~nodes ~@body)))

(defn edge [from to & props]
  {:pre [(contains? (apply hash-map props) :label)]}
  (apply assoc-edge! :graph from to props))

(defn remove-edge [from to]
  (delete-edge! :graph from to))

