(ns plasma.query
  (:use [plasma core operator]
        [jiraph graph]
        [lamina core])
  (:require [clojure (zip :as zip)]
            [logjam.core :as log]))

(log/channel :query)

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

(defn- where-predicate
  "Converts a query where predicate form into a predicate object."
  [form]
  (let [op (first form)]
    (cond
      (#{'= '< '> '>= '<=} op) (basic-pred form)
      :default (throw (Exception. (str "Unknown operator in where clause: " op))))))

(defn- where->predicates
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

(defn- plan-op
  [op & args]
  {:type op
   :id (uuid)
   :args (vec args)})

(defn- path-start
  "Determines the starting operator for a single path component,
  returning the start-op and rest of the path."
  [{:keys [pbind ops] :as plan} start path]
  (log/to :query "path-start: " start " -> " (get pbind start))
  (cond
    ; path starting with a keyword means start at the root
    (keyword? start) [(plan-op :parameter ROOT-ID) path]

    ; starting with a symbol, refers to a previous bind point in the query
    (and (symbol? start)
         (contains? pbind start)) [(get ops (get pbind start)) (next path)]

    ; starting with the UUID of a node starts at that node"
    (node-exists? :graph start) [(plan-op :parameter start) (next path)]))

(defn- path-plan
  "Creates the query-plan operator tree to implement a path traversal."
  [start path]
  (log/to :query "start: " start)
  (let [start-id (:id start)]
    (loop [root-id start-id
           src-id  start-id
           path path
           ops {start-id start}]
      (log/to :query "ops: " (map :op (vals ops)))
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

(defn- traversal-path
  "Takes a traversal-plan and a single [bind-name [path ... segment]] pair and
  adds the traversal to the plan.
  "
  [plan [bind-name path]]
  (log/to :query "path: " path)
;  (log/to :query "ops: " (map :op (vals (:ops plan))))
  (let [start (first path)
        [query-root path] (path-start plan start path)
        [root-op-id path-ops] (path-plan query-root path)
        root-op (get path-ops root-op-id)
        ops (merge (:ops plan) path-ops)
        _ (log/to :query "root-op: " root-op)
        bind-op (if (= :join (:type root-op))
                  (second (:args root-op))
                  (:id root-op))
        plan (assoc-in plan [:pbind bind-name] bind-op)]
    (assoc plan
           :ops ops
           :root root-op-id)))

(defn- traversal-tree
  "Convert a seq of [bind-name [path ... segment]] pairs into a query-plan
  representing the query operators implementing the corresponding path
  traversal.

  input: [(a [:foo :bar])
          (b [a :baz :zam])]"
  [paths]
  (reduce traversal-path
          {:ops {} :pbind {} :root nil :params {}}
          paths))

(defn- selection-ops
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
        (log/to :query "pred: " pred)
        (let [select-key (get-in plan [:pbind binding])
              op (plan-op :select (:root plan) select-key pred)]
          (assoc-in (assoc plan :root (:id op))
                    [:ops (:id op)] op)))
      plan
      flat-preds)))

(defn- projection
  "Add the projection operator node to produce the final result nodes for a query."
  [plan body]
  (let [params (:pbind plan)
        res-sym (last body)
        res-id (if (and (symbol? res-sym)
                        (contains? params res-sym))
                 (get params res-sym)
                 (second (first params)))
        result-op (get (:ops plan) res-id)
        _ (log/to :query "res-op: " result-op)
        proj-op (plan-op :project (:root plan) res-id)
        ops (assoc (:ops plan) (:id proj-op) proj-op)]
    (assoc plan
           :root (:id proj-op)
           :ops ops)))

(defn parameterized
  "Add the parameter map for a query."
  [plan]
  (let [param-ops (filter #(= :parameter (:type %)) (vals (:ops plan)))
        ;_ (log/to :query "param-ops: " param-ops)
        param-map (reduce (fn [mem op]
                            (assoc mem
                                   (first (:args op)) (:id op)))
                          {}
                          param-ops)]
    (assoc plan :params param-map)))

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
        paths (vec (map vec (partition 2 bindings)))
        trav-tree (traversal-tree paths)
        preds (where->predicates (where-form body))
        plan (merge {:type :path-query
                     :paths paths
                     :filters preds}
                    trav-tree)
        plan (selection-ops plan preds)
        plan (projection plan body)
        plan (parameterized plan)]
    plan))

(defn query?
  [q]
  (and (associative? q)
       (= :path-query (:type q))))

(defmacro path [& args]
  `(path* (quote ~args)))

(defn optimize-query-plan
  [plan]
  plan)

; TODO: Deal with nil receive op...
(defn- op-node
  "Instantiate an operator node based on a query node."
  [plan op-node]
  (let [{:keys [type id args]} op-node
        op-name (symbol (str (name type) "-op"))
        op-fn (ns-resolve 'plasma.operator op-name)]
    (case type
      :traverse
      (apply op-fn id nil args)

      :join
      (apply op-fn id (map plan args))

      :parameter
      (apply op-fn id args)

      :project
      (op-fn id (get plan (first args)) (second args))

      :aggregate
      (apply op-fn id (map #(get plan %) args))

      :receive
      (op-fn id)

      :send
      (op-fn id)

      :select
      (let [[left skey pred] args
            left (get plan left)]
        (op-fn id left skey pred))
    )))

(defn- ready-op?
  "Check whether an operator's child operators have been instantiated if it has children, 
  to see whether it is ready to be instantiated."
  [plan op]
  (let [child-ids (filter #(and (uuid? %) (not= ROOT-ID %)) (:args op))]
    (if (empty? child-ids)
      true
      (every? #(contains? plan %) child-ids))))

(defn- build-query
  "Iterate over the query operators, instantiating each one after its dependency
  operators have been instantiated."
  [plan]
  (loop [tree     {}
         ops (set (keys (:ops plan)))]
    (if (empty? ops)
      tree
      (let [_ (log/to :op "ops-count: " (count (:ops plan)))
            op-id (first (filter #(ready-op? tree (get (:ops plan) %)) ops))
            _ (log/to :op "build-query op-id: " op-id)
            op (get (:ops plan) op-id)
            _ (log/to :op "build-query op: " op)
            tree (assoc tree (:id op)
                        (op-node tree op))]
        (if (nil? op)
          nil
          (recur tree (disj ops op-id)))))))

(defn query-tree
  "Convert a query plan into a query execution tree."
  [plan]
  (let [tree (build-query plan)]
    {:type :query-tree
     :ops tree
     :root (:root plan)
     :params (:params plan)}))

(defn run-query
  "Execute a query by feeding parameters into a query operator tree."
  [tree param-map]
  (doseq [[param-name param-id] (:params tree)]
    (let [param-val (if (contains? param-map param-name)
                      (get param-map param-name)
                      param-name)
          param-op (get-in tree [:ops param-id])]
      (enqueue (get param-op :in) param-val))))

(defn query-results
  [tree & [timeout]]
  (let [timeout (or timeout 1000)]
    (channel-seq (get-in (:ops tree) [(:root tree) :out]) timeout)))

(defn query
  [plan & [param-map]]
  (log/to :op "query: " plan)
  (assert (query? plan))
  (let [tree (query-tree plan)
        param-map (or param-map {})]
    (run-query tree param-map)
    (doall (query-results tree))))
