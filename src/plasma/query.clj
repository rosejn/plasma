(ns plasma.query
  (:use jiraph protobuf
;        (clojure.contrib pprint seq-utils)
;        vijual
        ))

;(defn append-actions [layer id & args]
;    (conj-node! :actions id layer [(into [jiraph/*callback* id] args)]))

;(defgraph LOCAL
;    :path "db" :proto jiraph.Proto$Node :create true :bnum 1000000
;    (layer :overtone :store-length-on-append true))
(def ROOT-ID "ROOT")
(def LOCAL nil)

(def DB-PATH "db")
(def *current-query* nil)
(def *current-binding* nil)
(def *current-path* nil)
(def *current-edge-preds* nil)
(def *proxy-results* nil)

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

(defn where-form [body]
  (let [where? #(and (list? %) (= 'where (first %)))
        where (first (filter where? body))]
    where))

(defn query* [q]
  (let [[bindings body]
        (cond
          (and (vector? (first q))  ; (query [:a :b :c])
               (keyword? (ffirst q))) (let [res (gensym 'result)]
                                        [(vector res (first q)) (list res)])

          (and (vector? (first q))  ; (query [foo [:a :b :c]])
               (vector? (second (first q)))) [(first q) (rest q)]

          (symbol? (ffirst q)) [[] q]
          :default (throw
                     (Exception.
                       "Invalid query expression:
                       Missing either a binding or a query operator.")))
        where (where-form body)]
    {:type ::query
     :bindings (partition 2 bindings)
     :where where
     :body body}))

(defmacro query [& args]
  `(query* (quote ~args)))

(defn query? [obj]
  (and
    (associative? obj)
    (= :plasma.core/query (:type obj))))

; TODO: Use a multi-method that dispatches off aspects of the URL to
; create the connection and handle serialization
(defn proxy-connection [url]
  (let [sender nil ;(peer-sender url)
        serializer (fn [v] v)]
    {:type ::proxy-connection
     :url url
     :sender sender
     :serializer serializer}))

(defn send-query [con q]
  (let [{:keys [sender serializer]} con]
    (sender (serializer q))))

(defn proxy-sub-query [src-node]
  (let [query *current-query*
        bind-name *current-binding*
        trimmed-bindings (drop-while #(not (= bind-name (first %))) (:bindings query))
        trimmed-bindings (into [] (cons [*current-binding* *current-edge-preds*]
                                        (next trimmed-bindings)))
        trimmed-query (assoc query :bindings trimmed-bindings)
        con (proxy-connection (:proxy src-node))]
    (send-query con trimmed-query)))

(defn traverser
  "Takes a src node and an edge predicate as arguments, and it returns a seq of
  nodes on the target end of edges for which the predicate returns true."
  [src-id edge-pred]
  (let [edge-pred-fn (if (keyword? edge-pred)
                    #(= edge-pred (:label %1))
                    edge-pred)
        edges (vals (get-edges :graph src-id))]
    (if (proxy-node? src-id)
      (do
        (set! *proxy-results* (conj *proxy-results*
                                    (proxy-sub-query (find-node src-id))))
        [])
      (map :to-id (filter edge-pred-fn edges)))))

(defn joiner
  "Returns the nodes traversed by following edge-pred from each node in src-seq."
  [src-seq edge-pred]
  (mapcat #(traverser % edge-pred) src-seq))

(defn path-seq
  "Returns a seq of target nodes reached by path starting from the src node."
  [src path]
  (let [start (cond
                (seq? src) (joiner src (first path))
                (node-exists? :graph src) (traverser src (first path)))]
    (loop [src-nodes start
           edge-preds (next path)]
      (if edge-preds
        (binding [*current-edge-preds* edge-preds]
          (recur (joiner src-nodes (first edge-preds))
                 (next edge-preds)))
        src-nodes))))

;    (reduce (fn [src-nodes edge-pred]
;              (joiner src-nodes edge-pred))
;            start
;            (next path))))

(defn pred-pass? [node pred]
  (let [{:keys [operator property value]} pred]
    (if-let [pval (get node property)]
      (condp = operator
        '=  (=  pval value)
        '<  (<  pval value)
        '>  (>  pval value)
        '<= (<= pval value)
        '>= (>= pval value))
      false)))

(defn predicate-filter [preds]
  (fn [id]
    (let [n (find-node id)]
    (every? #(pred-pass? n %) preds))))

(defn selector
  "Filter nodes using a set of predicates. (From the where clause)"
  [predicates path-seq]
  (filter (predicate-filter predicates) path-seq))

(defn path-map
  "returns a map of bind-name -> path-seq pairs

  * a path starting with a keyword always starts at the root
  * a path starting with a symbol, refers to a previous bind point in the query
  * a path starting with the ID of a node in the graph starts at that node"
  [bindings predicates]
  (reduce (fn [paths [bind-name path]]
            (let [start (first path)
                  [query-root path]
                  (cond
                    (keyword? start) [ROOT-ID path]
                    (and (symbol? start)
                         (contains? paths start)) [(get paths start) (next path)]
                    (node-exists? :graph start) [start (next path)])
                  p-seq (binding [*current-binding* bind-name
                                  *current-path* path]
                          (path-seq query-root path))
                  result-path (if-let [preds (get predicates bind-name)]
                                (selector preds p-seq)
                                p-seq)]
              (assoc paths bind-name result-path)))
          {}
          bindings))

(defn path-map-old
  "returns a map of bind-name -> path-seq pairs

  * a path starting with a keyword always starts at the root
  * a path starting with a symbol, refers to a previous bind point in the query
  * a path starting with the ID of a node in the graph starts at that node"
  [bindings predicates]
  (reduce (fn [paths [bind-name path]]
            (let [start (first path)
                  [query-root path]
                  (cond
                    (keyword? start) [ROOT-ID path]
                    (and (symbol? start)
                         (contains? paths start)) [(get paths start) (next path)]
                    (node-exists? :graph start) [start (next path)])
                  p-seq (path-seq query-root path)]
              (assoc paths bind-name
                     (if-let [preds (get predicates bind-name)]
                       (filter (predicate-filter preds) p-seq)
                       p-seq))))
          {}
          bindings))

(defn basic-pred [form]
  (let [[op a b] form
        [bind-symbol property value] (if (list? a)
                                       [(second a) (first a) b]
                                       [(second b) (first b) a])]
    {:binding bind-symbol
     :operator op
     :property property
     :value value}))

(defn where-predicate [form]
  (let [op (first form)]
    (cond
      (#{'= '< '> '>= '<=} op) (basic-pred form)
      :default (throw (Exception. (str "Unknown operator in where clause: " op))))))

(defn run-query
  "Run a query."
  [query]
  (binding [*current-query* query
            *proxy-results* []]
    (let [pred-list (map where-predicate (next (:where query)))
          predicates (reduce (fn [pred-map pred]
                               (assoc pred-map
                                      (:binding pred)
                                      (conj (get pred-map (:binding pred))
                                            (dissoc pred :binding))))
                             {}
                             pred-list)
          paths (path-map (:bindings query) predicates)
          result-key (last (:body query))
          proxy-results (concat (map (fn [prom] @prom) *proxy-results*))]
      (flatten (lazy-cat (get paths result-key) proxy-results)))))

; NOTE: probably breaks if we add cycles...  Should implement a normal
; BFS using a visited set or something...
(defn all-nodes []
  (tree-seq (fn [_] true) #(keys (get-edges :graph %)) ROOT-ID))

(comment
(defn- vijual-tree [cur-id]
  (let [n (find-node cur-id)
        children (keys (get-edges :graph cur-id))]
    (into [(:label n)] (map #(vijual-tree %) children))))

(defn print-tree []
  (draw-tree [(vijual-tree ROOT-ID)]))

(defn edge-pairs []
  (partition 2 (flatten
                 (map (fn [id] (interleave (cycle [id]) (keys (get-edges :graph id))))
                      (all-nodes)))))

(defn print-graph []
  (draw-directed-graph [(edge-pairs)] (map #(:label (find-node %)) (all-nodes))))
)
