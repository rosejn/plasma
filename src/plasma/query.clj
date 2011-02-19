(ns plasma.query
  (:use [plasma util core operator]
        [jiraph graph]
        [lamina core])
  (:require [clojure (zip :as zip)]
            [logjam.core :as log]))

(log/channel :query :debug)

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

  For example, query for the tracks with a score greater than 0.8:
    (path [track [:music :tracks]]
      (where (> (:score track) 0.8)))

  "
  [plan form]
  (let [pred-list (map where-predicate (next form))
        predicates (reduce (fn [pred-map pred]
                             (assoc pred-map
                                    (:binding pred)
                                    (conj (get pred-map (:binding pred))
                                          (dissoc pred :binding))))
                           {}
                           pred-list)]
    (assoc plan :filters predicates)))

(defn plan-op
  [op & args]
  {:type op
   :id (uuid)
   :args (vec args)})

(defn- path-start-src
  "Determines the starting operator for a single path component,
  returning the start-op and rest of the path."
  [plan jbind path]
  (let [start (first path)
        {:keys [pbind ops]} plan]
    (log/to :query "path-start-src: " start " -> " (get jbind start))
    (cond
      ; path starting with a keyword means start at the root
      (keyword? start)
      (let [root-op (plan-op :parameter ROOT-ID)]
        [root-op (:id root-op)])

      ; starting with a symbol, refers to a previous bind point in the query
      (and (symbol? start)
           (and
             (contains? jbind start)
             (contains? pbind start)))
        [(get ops (get jbind start))
         (get pbind start)]

      ; starting with the UUID of a node starts at that node"
      (node-exists? :graph start)
      (let [root-op (plan-op :parameter start)]
        [root-op (:id root-op)]))))

(defn- path-plan
  "Creates the query-plan operator tree to implement a path traversal."
  [src-id root-op path]
  (let [root-id (:id root-op)]
    (loop [root-id root-id
           src-id  src-id
           path path
           ops {root-id root-op}]
      (log/to :query "root: " root-id " src: " src-id " path: " path)
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
  [[plan jbind] [bind-name path]]
  (log/to :query "path: " path)
;  (log/to :query "ops: " (map :op (vals (:ops plan))))
  (let [[start-op src-key] (path-start-src plan jbind path)
        path (if (keyword? (first path))
               path (next path))
        [root-op-id path-ops] (path-plan src-key start-op path)
        root-op               (get path-ops root-op-id)
        ops                   (merge (:ops plan) path-ops)
        _ (log/to :query "root-op: " root-op)
        bind-op (if (= :join (:type root-op))
                  (second (:args root-op))
                  (:id root-op))
        plan (assoc-in plan [:pbind bind-name] bind-op)
        jbind (assoc jbind bind-name (:id root-op))]
    [(assoc plan
           :ops ops
           :root root-op-id)
     jbind]))

(defn- traversal-tree
  "Convert a seq of [bind-name [path ... segment]] pairs into a query-plan
  representing the query operators implementing the corresponding path
  traversal.

  input: [(a [:foo :bar])
          (b [a :baz :zam])]"
  [plan paths]
  (first (reduce traversal-path [plan {}] paths)))

(defn- selection-ops
  "Add the selection operators corresponding to a set of predicates
  to a query plan."
  [plan]
  (let [preds (:filters plan)
        ; flatten with binding name because each bind-point could
        ; have more than one predicate attached (and ...)
        flat-preds (partition 2 (mapcat (fn [[bind pred-seq]]
                         (interleave (repeat bind)
                                     pred-seq)) preds))]
    (reduce
      (fn [plan [binding pred]]
        (log/to :query "pred: " pred)
        (let [select-key (get-in plan [:pbind binding])
              p-op (plan-op :property (:root plan) select-key [(:property pred)])
              s-op (plan-op :select (:id p-op) select-key pred)
              ops (assoc (:ops plan)
                         (:id p-op) p-op
                         (:id s-op) s-op)]
          (assoc plan
                 :root (:id s-op)
                 :ops ops)))
      plan
      flat-preds)))

(comment defn- projection
  "Add the projection operator node to produce the final result nodes for a query."
  [plan body]
  (log/to :query "projection (body): " body "\n"
          (get (:ops plan) (get-in plan [:pbind (last body)])))
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

(defn- load-props
  [plan bind-sym properties]
  (let [bind-op (get (:pbind plan) bind-sym)
        root-op (:root plan)
        prop-op (plan-op :property root-op bind-op properties)]
    (assoc plan
           :root (:id prop-op)
           :ops (assoc (:ops plan) (:id prop-op) prop-op))))

(defn project
  "Project the incoming path-tuples so the result will be either a set of node UUIDs or a set of node maps with properties.

  (let [q (path [people [:app :social :friends]])]
    ...
  
    ; get the UUIDs of people nodes
    (project q 'people) 
  
    ; get the given set of props for each person node
    (project 'people :name :email :age))
    
  "
  [plan bind-sym & properties]
  (log/to :query "[pject] bind-sym: " bind-sym)
  (let [props? (not (empty? properties)) 
        plan (if props?
               (load-props plan bind-sym properties)
               plan)
        bind-op (get (:pbind plan) bind-sym)
        root-op (:root plan)
        proj-op (plan-op :project root-op bind-op props?)]
    (assoc plan
           :root (:id proj-op)
           :ops (assoc (:ops plan) (:id proj-op) proj-op))))

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

(defn- plan-receiver
  [plan]
  (let [op (plan-op :receive (:root plan))
        ops (assoc (:ops plan) (:id op) op)]
    (assoc plan
           :ops ops
           :root (:id op))))

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
        plan {:type :query
              :root nil    ; root operator id
              :params {}   ; param-name -> parameter-op
              :ops {}      ; id         -> operator
              :filters {}  ; binding    -> predicate
              :pbind {}    ; symbol     -> traversal       (path binding)
              }]
    (-> plan
      (traversal-tree paths)
      (where->predicates (where-form body))
      (plan-receiver)
      (selection-ops)
      (parameterized))))

(defmacro path [& args]
  `(path* (quote ~args)))

(defn query?
  [q]
  (and (associative? q)
       (= :query (:type q))))

(defn sub-query?
  [q]
  (and (associative? q)
       (= :sub-query (:type q))))

(defn sort*
  [plan sort-var sort-prop & [order]]
  (let [order (or order :asc)
        {:keys [root ops]} plan
        sort-key (get (:pbind plan) sort-var)
        p-op (plan-op :property (:root plan) sort-key [sort-prop])
        s-op (plan-op :sort (:id p-op) sort-key sort-prop order)
        ops (assoc ops
                   (:id p-op) p-op
                   (:id s-op) s-op)]
    (assoc plan
           :root (:id s-op)
           :ops ops)))

(defn optimize-query-plan
  [plan]
  plan)

; TODO: Deal with nil receive op...
(defn- op-node
  "Instantiate an operator node based on a query node."
  [ops op-node]
  (let [{:keys [type id args]} op-node
        op-name (symbol (str (name type) "-op"))
        op-fn (ns-resolve 'plasma.operator op-name)]
    (log/format :op "[op-node] type: %s args: %s " type args)
    (case type
      :traverse
      (apply op-fn id nil args)

      :join
      (apply op-fn id (map ops args))

      :parameter
      (apply op-fn id args)

      :project
      (apply op-fn id (get ops (first args)) (rest args))

      :aggregate
      (apply op-fn id (map #(get ops %) args))

      :receive
      (op-fn id (get ops (first args)))

      :send
      (do
        (log/to :query "[send-op] op-node: " (str op-node))
        (op-fn id (get ops (first args)) (second args)))

      :select
      (let [[left skey pred] args
            left (get ops left)]
        (op-fn id left skey pred))

      :property
      (apply op-fn id (get ops (first args)) (rest args))
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
      (let [_ (log/to :op-b "ops-count: " (count (:ops plan)))
            op-id (first (filter #(ready-op? tree (get (:ops plan) %)) ops))
            _ (log/to :op-b "build-query op-id: " op-id)
            op (get (:ops plan) op-id)
            _ (log/to :op-b "build-query op: " op)
            tree (assoc tree (:id op)
                        (op-node tree op))]
        (if (nil? op)
          nil
          (recur tree (disj ops op-id)))))))

(defn has-projection?
  "Check whether a query plan contains a project operator."
  [plan]
  (if (first (filter #(= :project (:type %)) (vals (:ops plan))))
    true
    false))

(defn- with-result-project
  "If an explicity projection has not been added to the query plan
  then by default we project on the final element of the path query."
  [plan]
  (if (has-projection? plan)
    plan
    (project plan (ffirst (:pbind plan)))))

(defn query-tree
  "Convert a query plan into a query execution tree."
  [plan]
  (let [plan (with-result-project plan)
        tree (build-query plan)]
    {:type :query-tree
     :ops tree
     :root (:root plan)
     :params (:params plan)}))

(defn run-query
  "Execute a query by feeding parameters into a query operator tree."
  [tree param-map]
  (unless *graph*
    (throw (Exception. "Cannot run a query without binding a graph.
\nFor example:\n\t(with-graph G (query q))\n")))
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
  (log/to :query "[QUERY] ------------------------------------------------\n" plan)
  (assert (query? plan))
  (let [tree (query-tree plan)
        param-map (or param-map {})]
    (run-query tree param-map)
    (doall (query-results tree))))

(defn- with-send-channel
  "Append channel to the end of each send operator's args list."
  [plan ch]
  (let [ops (map (fn [op]
                   (if (= :send (:type op))
                     (assoc op :args (concat (:args op) [ch]))
                     op))
                 (vals (:ops plan)))
        ops (reduce (fn [mem op]
                      (assoc mem (:id op) op))
                    {}
                    ops)]
    (log/to :query "[with-send-channel] ops: " ops)
    (doseq [op (filter #(= :send (:type %)) ops)]
      (log/to :query "[with-send-channel] op: " op))
    (assoc plan :ops ops)))

(defn sub-query
  [ch plan & [param-map]]
  (assert (sub-query? plan))
  (log/to :query "[SUB-QUERY] ------------------------------------------------\n" plan)
  (log/format :query "channel: %s" ch)
  (let [plan (with-send-channel plan ch)
        tree (query-tree plan)]
    (run-query tree {})))

; TODO: Add a simple query type (and maybe operator) to return a single node by UUID

