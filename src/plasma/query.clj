(ns plasma.query
  (:use [plasma core operator]
        jiraph))

(defn where-form [body]
  (let [where? #(and (list? %) (= 'where (first %)))
        where (first (filter where? body))]
    where))

(defn- basic-pred
  [form]
  (let [[op a b] form
        [bind-symbol property value] (if (list? a)
                                       [(second a) (first a) b]
                                       [(second b) (first b) a])]
    {:binding bind-symbol
     :operator op
     :property property
     :value value}))

(defn where-predicate
  "Converts a query where predicate form into a predicate object."
  [form]
  (let [op (first form)]
    (cond
      (#{'= '< '> '>= '<=} op) (basic-pred form)
      :default (throw (Exception. (str "Unknown operator in where clause: " op))))))

(defn where->predicates 
  "Converts a where form in a path query to a set of predicate maps.

   Where forms are structured like so:
    (where (op val (:property bind-name)))
  "
  [form]
  (let [pred-list (map where-predicate (next form))]
    (reduce (fn [pred-map pred]
              (assoc pred-map
                     (:binding pred)
                     (conj (get pred-map (:binding pred))
                           (dissoc pred :binding))))
            {}
            pred-list)))

(defn plan-op
  [op & {:as args}]
  (merge {:op op :id (uuid)} args))

(defn path-start
  "Determines the starting operator for a single path component, 
  returning the start-op and rest of the path."
  [plan start path]
  (cond
    ; path starting with a keyword means start at the root
    (keyword? start) [(plan-op 'param-op :init ROOT-ID) path]

    ; starting with a symbol, refers to a previous bind point in the query
    (and (symbol? start)
         (contains? plan start)) [(get plan start) (next path)]

    ; starting with the UUID of a node starts at that node"
    (node-exists? :graph start) [(plan-op 'param-op :init start) (next path)]))

(defn path-plan
  [start path]
  (second
    (reduce (fn [[root last-trav] segment]
            (let [trav (plan-op 'traverse-op :src last-trav :edge segment)]
              [(plan-op 'join-op :left start :right trav) trav]))
          [start start]
          path)))

(defn traversal-path
  "Takes a traversal-plan and a single [bind-name [path ... segment]] pair and
  adds the traversal to the plan.
  "
  [plan [bind-name path]]
  (let [start (first path)
        [query-root path] (path-start plan start path)
        p-plan (path-plan start path)]
    (assoc plan bind-name p-plan)))

(defn traversal-tree
  "Convert a seq of [bind-name [path ... segment]] pairs into a query-plan
  representing the query operators implementing the corresponding path
  traversal.

  input: [(a [:foo :bar])
          (b [a :baz :zam])]"
  [paths]
  (reduce traversal-path {} paths))

(defn path-map
  "Converts from a seq of path bindings to an operator description tree.

  "
  [bindings])

(defn path* [q]
  (let [[bindings body]
        (cond
          (and (vector? (first q))  ; (path [:a :b :c])
               (keyword? (ffirst q))) (let [res (gensym 'result)]
                                        [(vector res (first q)) (list res)])

          (and (vector? (first q))  ; (path [foo [:a :b :c]])
               (vector? (second (first q)))) [(first q) (rest q)]

          (symbol? (ffirst q)) [[] q]
          :default (throw
                     (Exception.
                       "Invalid path expression:
                       Missing either a binding or a path operator.")))
        paths (partition 2 bindings)
        trav-tree (traversal-tree paths)
        pathmap (path-map paths)
        preds (where->predicates (where-form body))]
    {:type ::path
     :paths paths
     :pmap pathmap
     :filters preds}))

(defmacro path [& args]
  `(path* (quote ~args)))

(defn optimize-query-plan [plan]
  plan)

(defn build-query [plan] nil)
