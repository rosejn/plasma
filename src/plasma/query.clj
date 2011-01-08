(ns plasma.query
  (:use [plasma core operator]
        [jiraph graph]))

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
  [op & args]
  {:type (keyword "plasma.operator" (name op))
   :id (uuid)
   :args args})

(defn path-start
  "Determines the starting operator for a single path component,
  returning the start-op and rest of the path."
  [{:keys [pbind ops] :as plan} start path]
  ;(println "path-start: " start " -> " (get pbind start))
  (cond
    ; path starting with a keyword means start at the root
    (keyword? start) [(plan-op :parameter ROOT-ID) path]

    ; starting with a symbol, refers to a previous bind point in the query
    (and (symbol? start)
         (contains? pbind start)) [(get ops (get pbind start)) (next path)]

    ; starting with the UUID of a node starts at that node"
    (node-exists? :graph start) [(plan-op :parameter start) (next path)]))

(defn path-plan
  "Creates the query-plan operator tree to implement a path traversal."
  [start path]
  ;(println "start: " start)
  (let [start-id (:id start)]
    (loop [root-id start-id
           src-id  start-id
           path path
           ops {start-id start}]
      ;(println "ops: " (map :op (vals ops)))
      (if path
        (let [trav (plan-op :traverse src-id (first path))
              t-id (:id trav)
              join (plan-op :join root-id t-id)
              j-id (:id join)]
          (recur j-id t-id (next path)
                 (assoc ops
                        j-id join
                        t-id trav)))
        [root-id ops]))))

(defn traversal-path
  "Takes a traversal-plan and a single [bind-name [path ... segment]] pair and
  adds the traversal to the plan.
  "
  [plan [bind-name path]]
  ;(println "path: " path)
;  (println "ops: " (map :op (vals (:ops plan))))
  (let [start (first path)
        [query-root path] (path-start plan start path)
        [root-op path-ops] (path-plan query-root path)
        ops (merge (:ops plan) path-ops)
        plan (assoc-in plan [:pbind bind-name] root-op)]
    (assoc plan
           :ops ops
           :root root-op)))

(defn traversal-tree
  "Convert a seq of [bind-name [path ... segment]] pairs into a query-plan
  representing the query operators implementing the corresponding path
  traversal.

  input: [(a [:foo :bar])
          (b [a :baz :zam])]"
  [paths]
  (reduce traversal-path
          {:ops {} :pbind {} :root nil :params {}}
          paths))

(defn selection-ops
  "Add the selection operators corresponding to a set of predicates
  to a query plan."
  [plan preds]
  (let [; flatten with binding name because each bind-point could
        ; have more than one predicate attached (and ...)
        flat-preds (partition 2 (mapcat (fn [[bind pred-seq]]
                         (interleave (repeat bind)
                                     pred-seq)) preds))]
    (reduce
      (fn [plan [binding pred]]
        (println "pred: " pred)
        (let [select-key (get-in plan [:pbind binding])
              op (plan-op :select (:root plan) select-key pred)]
          (assoc-in (assoc plan :root (:id op))
                    [:ops (:id op)] op)))
      plan
      flat-preds)))

(defn path*
  "Helper function to 'parse' a path expression and generate an initial
  query plan."
  [q]
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
        preds (where->predicates (where-form body))
        plan (merge {:type ::path
                     :paths paths
                     :filters preds}
                    trav-tree)
        plan (selection-ops plan preds)]
    plan))

(defmacro path [& args]
  `(path* (quote ~args)))

(defn optimize-query-plan
  [plan]
  plan)

; TODO: Deal with nil receive op...
(defn op-node
  "Instantiate an operator node based on a plan node."
  [plan op-node]
  (let [{:keys [ops]} plan
        {:keys [type id op args]} op-node
        op-name (symbol (str (name op) "-op"))
        op-fn (ns-resolve 'plasma.operator op-name)]
    (case type
      :plasma.operator/traverse
      (apply op-fn id nil args)

      :plasma.operator/join
      (apply op-fn id (map #(get ops %) args))

      :plasma.operator/project
      :plasma.operator/aggregate
      :plasma.operator/parameter
      :plasma.operator/receive
      :plasma.operator/send
      :plasma.operator/select
    )))

(defn build-query
  "Depth first traversal of query plan, resulting in a fully realized
  operator tree."
  [{:keys [ops pbind root] :as plan}]
  (let [receiver (receive-op (uuid))]))

;(filter (fn [arg]
;          (or
;           ()
;           (and (uuid? arg)
;                (contains? query-ops arg)))
;        (:args op))
