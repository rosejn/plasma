(ns plasma.query.plan
  (:require [clojure (set :as set)]
            [logjam.core :as log])
  (:use [plasma util graph]
        [plasma.query operator helpers]))

(defn path-start-src
  "Determines the starting operator for a single path component,
  returning the start-op and rest of the path."
  [plan jbind path-seg]
  (let [start (first path-seg)
        {:keys [pbind ops]} plan]
    (log/to :query "path-start-src: " start " -> " (get jbind start))
    (log/to :query "jbind: " jbind "\npbind: " pbind)
    (cond
      ; path starting with a keyword means start at the root
      (keyword? start)
      (let [root-op (plan-op :parameter
                             :args [ROOT-ID])]
        [root-op (:id root-op)])

      ; starting with a symbol, refers to a previous bind point in the query
      (and (symbol? start)
           (and
             (contains? jbind start)
             (contains? pbind start)))
        [(get ops (get jbind start))
         (get pbind start)]

      ; starting with the UUID of a node starts at that node"
      (uuid? start)
      (let [root-op (plan-op :parameter
                             :args [start])]
        [root-op (:id root-op)]))))

(defn path-plan
  "Creates the query-plan operator tree to implement a path traversal."
  [src-id root-op path]
  (let [root-id (:id root-op)]
    (loop [root-id root-id
           src-id  src-id
           path path
           ops {root-id root-op}]
      (log/to :query "root: " root-id " src: " src-id " path: " path)
      (if path
        (let [trav (plan-op :traverse
                            :args [src-id (first path)])
              t-id (:id trav)
              join (plan-op :join
                            :deps [root-id t-id])
              j-id (:id join)]
          (recur j-id t-id (next path)
                 (assoc ops
                        j-id join
                        t-id trav)))
        [root-id ops]))))

(defn traversal-path
  "Takes a traversal-plan and a single [bind-name [path ... segment]] pair and
  adds the traversal to the plan.  Note, jbind is a map of identified path segment to
  join operator at the root of that segment's traversal tree, which is where we need
  to get values if referring to a previous path segment.
  "
  [[plan jbind] [bind-name segment]]
  (log/to :query "path: " segment)
  (let [[start-op src-key] (path-start-src plan jbind segment)
        segment (if (keyword? (first segment))
                  segment
                  (next segment))
        [root-op-id path-ops] (path-plan src-key start-op segment)
        root-op               (get path-ops root-op-id)
        ops                   (merge (:ops plan) path-ops)
        _ (log/to :query "root-op: " root-op)
        bind-op (if (= :join (:type root-op))
                  (second (:deps root-op))
                  (:id root-op))
        plan (assoc-in plan [:pbind bind-name] bind-op)
        jbind (assoc jbind bind-name (:id root-op))]
    [(assoc plan
           :ops ops
           :root root-op-id)
     jbind]))

(defn with-traversal-tree
  "Convert a seq of [bind-name [path ... segment]] pairs into a query-plan
  representing the query operators implementing the corresponding path
  traversal.

  input: [(a [:foo :bar])
          (b [a :baz :zam])]"
  [plan jbind paths]
  (first (reduce traversal-path [plan jbind] paths)))

(defn load-props
  "Add a property loading operator to the query plan to add property
  values to the run-time path-map for downstream operators to utilize.
  This is how, for example, select operators can access property values."
  [{root :root :as plan} bind-sym properties]
  (let [bind-op (get (:pbind plan) bind-sym)
        prop-op (plan-op :property
                         :deps [root]
                         :args [bind-op properties])]
    (append-root-op plan prop-op)))


(defn numerical-op
  [plan op-name op-var op-prop]
  (let [{:keys [root ops pbind] :as plan} (load-props plan op-var [op-prop])
        op-key (get pbind op-var)
        num-op (assoc (plan-op op-name
                               :deps [root]
                               :args [op-key op-prop])
                      :numerical? true)]
    (assoc plan
           :root (:id num-op)
           :ops (assoc ops
                       (:id num-op) num-op))))


(defn root-op
  "Returns the root operator for a query plan."
  [p]
  (get (:ops p) (:root p)))

(defn default-project-binding
  "Finds the default projection binding for a query.  If the path consisted
  of only one bound segment then it uses that one, otherwise it uses the last
  bound segment."
  [plan]
  (if (= 1 (count (:pbind plan)))
    (ffirst (:pbind plan))
    (first (last (:paths plan)))))

(defn with-param-map
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

(defn with-receive-op
  "Add a receive operator to the root of the query plan."
  [plan]
  (append-root-op plan (plan-op :receive
                                :deps [(:root plan)])))

(defn eval-path-binding
  "Iterate through the path expression evaluating all symbols that aren't
  from previous path binding vars."
  [paths]
  (vec (second
         (reduce
           (fn [[known-syms new-paths] [bsym path-segs]]
             (let [sym `'~bsym
                   known-syms (conj known-syms bsym)
                   segs (vec (map
                               (fn [seg]
                                 (if (known-syms seg)
                                   `'~seg
                                   `~seg)) path-segs))
                   new-paths (conj new-paths [sym segs])]
               [known-syms new-paths]))
           [#{} []]
           paths))))

(defn ops-of-type
  "Select all operator nodes of a given type."
  [plan type]
  (filter #(= type (:type %)) (vals (:ops plan))))

;TODO:
; * Combine property-ops for the same key
; * change :select oriented logic to filter, or change filter to select...
(defn optimize-plan
  [plan]
  ; Pull property and select ops to just below the join after their associated traversals
  (let [select-ops (ops-of-type plan :select)
        selects-up (reduce (fn [mem op]
                             (let [select-key (first (:args op))
                                   tgt-op (downstream-op-node mem select-key)]
                               (log/to :optimize "[optimize-plan] select-tgt: " tgt-op)
                               (reparent-op mem (:id op) (:id tgt-op))))
                           plan
                         select-ops)
        prop-ops (ops-of-type plan :property)
        props-up (reduce (fn [mem op]
                           (let [prop-key (first (:args op))
                                 tgt-op (downstream-op-node mem prop-key)]
                             (log/to :optimize "[optimize-plan] prop-key: " prop-key
                                     " prop-tgt: " tgt-op)
                             (if (= (first (:deps op)) (:id tgt-op))
                               mem
                               (reparent-op mem (:id op) (:id tgt-op)))))
                         selects-up
                         prop-ops)]
    props-up))

(defn op-node
  "Instantiate an operator node based on a query node."
  [plan ops recv-chan timeout op-node]
  (let [{:keys [type id deps args]} op-node
        deps-ops (map ops deps)
        _ (log/format :op-node "type: %s\nid: %s\ndeps: %s\nargs: %s " type id deps args)
        op-name (symbol (str (name type) "-op"))
        op-fn (ns-resolve 'plasma.query.operator op-name)]
    (case type
      :traverse
      (apply op-fn id plan recv-chan args)

      :join
      (apply op-fn id deps-ops)

      :parameter
      (apply op-fn id args)

      :receive
      (op-fn id (first deps-ops) recv-chan timeout)

      (apply op-fn id (first deps-ops) args))))

(def MAX-OPS 500)

(defn op-dep-list
  "Returns a sorted operator dependency list. (Starting at the leaves of the tree
  where the operators have no dependencies and iterating in towards the root.)"
  [plan]
  (let [ops (vals (:ops plan))
        leaves (filter #(empty? (:deps %)) ops)]
    (loop [sorted (vec leaves)
           others (set/difference (set ops) (set leaves))
           counter 0]
      (if (or (empty? others)
              (= counter MAX-OPS))
        sorted
        (let [sort-set (set (map :id sorted))
              ;_ (println "sort-set: " sort-set)
              next-level (filter #(set/subset? (set (:deps %))
                                               sort-set)
                                 others)
              ;_ (println "next-level: " next-level)
              sorted (concat sorted next-level)]
              ;(println "sorted:" sorted)
          (recur
            sorted
            (set/difference others (set sorted))
            (inc counter)))))))

