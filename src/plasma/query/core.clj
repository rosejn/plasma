(ns plasma.query.core
  (:use
    [plasma util]
    [plasma.query plan exec operator helpers expression])
  (:require
    [clojure (zip :as zip)]
    [clojure (set :as set)]
    [logjam.core :as log]
    [lamina.core :as lamina]))

(def MAX-QUERY-TIME (* 2 1000)) ; in ms
(def PROMISE-WAIT-TIME 100)

(log/channel :query :debug)
(log/channel :optimize :query)
(log/channel :build :query)

(defn query?
  [q]
  (and (associative? q)
       (= :query (:type q))))

(defn where*
  [plan expr-vars expr]
  (let [var-props (reduce
                    (fn [prop-map exv]
                      (update-in prop-map [(symbol (:pvar exv))]
                                 conj (:property exv)))
                    {} expr-vars)
        plan (reduce #(load-props %1 %2 (get var-props %2))
                     plan
                     (keys var-props))
        expr-vars (vec (map #(assoc % :bind-key (get (:pbind plan)
                                                 (symbol (:pvar %))))
                        expr-vars))]
    (append-root-op plan {:type :filter
                          :id (uuid)
                          :deps [(:root plan)]
                          :args [expr-vars (str expr)]})))

(defmacro where
  [plan expr]
  (let [expr (rebind-expr-ops expr)]
    `(binding [*expr-vars* (atom [])]
       (let [evaled-expr# ~expr]
         (where* ~plan @*expr-vars* evaled-expr#)))))

(defn expr*
  [plan expr-vars expr-form])

(defmacro expr
  [plan form]
  (let [expr (rebind-expr-ops expr)]
    `(binding [*expr-vars* (atom [])]
       (let [evaled-expr# ~expr]
         (expr* ~plan @*expr-vars* evaled-expr#)))))

(defn project
  "Project the set of path results to maps containing specific properties of
  nodes along the path.

  (let [q (path [app    [:app :social]
                 person [:friends]])]
    ...

    ; get the UUIDs of person nodes
    (project q 'person)

    ; get the given set of props for each person node
    (project ['person :name :email :age])

    ; get properties from multiple nodes along the path
    (project ['app :label] ['person :name :email :age])

    ; and in case of conflicts, you can rename properties using :as
    (project ['app [:name :as :app-name]] ['person :name :email :age]))
  "
  [plan & args]
  (if-let [arg (some #(not (or (symbol? %)
                               (and (vector? %)
                                    (symbol? (first %)))))
                     args)]
    (throw (Exception.
             (format "Trying to project with invalid arguments.  Project requires either a path binding variable (symbol) or a vector containing a binding var and one or more keyword properties to project on.  (e.g. (project person ['article :author :title]))  Instead got: %s with bad arg: %s of type: %s" args arg (type arg)))))
  (let [projections (map #(if (symbol? %) [%] %) args)
        _ (log/to :query "[project] projections:" (seq projections))
        plan (doall
               (reduce
                 (fn [plan [bind-sym & properties]]
                   (if-not (empty? properties)
                     (let [props (doall (map #(if (vector? %) (first %) %) properties))]
                       (load-props plan bind-sym props))
                     plan))
                 plan projections))
        root-op (:root plan)
        projections (doall
                      (map (fn [[bind-sym & props]]
                             (let [project-key (get (:pbind plan) bind-sym)]
                               (when (nil? project-key)
                                 (throw (Exception. (format "Error trying to project using an invalid path binding: %s" bind-sym))))
                               (let [props (or props [:id])
                                     props (doall
                                             (map #(if (vector? %)
                                                   [(first %) (nth % 2)]
                                                   [% %])
                                                props))]
                                 (concat [project-key] props))))
                           projections))
        proj-op (plan-op :project
                         :deps [root-op]
                         :args [projections])]
    (append-root-op plan proj-op)))

(defn count*
  [{root :root :as plan}]
  (append-root-op plan
                  (assoc (plan-op :count
                                  :deps [root]
                                  :args [])
                         :numerical? true)))

(defn avg
  "Take the average of a set of property values bound to a path expression variable."
  [plan avg-var avg-prop]
  (numerical-op plan :average avg-var avg-prop))

(defn max*
  "Take the maximum of a set of property values bound to a path expression variable."
  [plan max-var max-prop]
  (numerical-op plan :max max-var max-prop))

(defn min*
  "Take the minimum of a set of property values bound to a path expression variable."
  [plan min-var min-prop]
  (numerical-op plan :min min-var min-prop))

(defn choose
  "Take N random elements from the result set."
  [{root :root :as plan} n]
  (append-root-op plan (plan-op :choose
                                :deps [root]
                                :args [n])))

(defn limit
  "Take the first N elements from the result set."
  [{root :root :as plan} n]
  (append-root-op plan (plan-op :limit
                                :deps [root]
                                :args [n])))

(defn group-by*
  "Group the result set by a node's property value."
  [plan group-bind group-prop]
  (let [{:keys [root pbind] :as plan} (load-props plan group-bind [group-prop])]
    (append-root-op plan (plan-op :group-by
                                  :deps [root]
                                  :args [(get pbind group-bind) group-prop]))))

(defn distinct*
  "Return only one element in the result set for each instance of a specific binding ID
  and/or property value(s)."
  [plan d-bind & props]
  (let [{:keys [root pbind] :as plan} (if (empty? props)
                                        plan
                                        (load-props plan d-bind props))]
    (append-root-op plan (plan-op :distinct
                                  :deps [root]
                                  :args [(get pbind d-bind) props]))))

(defn path*
  "Helper function to 'parse' a path expression and generate an initial
  query plan.  If a nested query is found as the first element of the path
  then this path will start from the results of the nested query."
  [paths body]
  (let [[[p1-bind [first-segment & other-segs]] & other-paths] paths
        [plan jbind paths]
        (if (query? first-segment)
          (let [plan first-segment
                sub-query-bind (default-project-binding plan)
                paths (cons [p1-bind (cons sub-query-bind other-segs)]
                            other-paths)
                plan (update-in plan [:paths] concat paths)
                jbind {sub-query-bind (:root plan)}]
            [plan jbind paths])
          [(with-meta
             {:type :query
              :id (uuid)
              :root nil    ; root operator id
              :params {}   ; param-name -> parameter-op
              :ops {}      ; id         -> operator
              :filters {}  ; binding    -> predicate
              :pbind {}    ; symbol     -> traversal       (path binding)
              :paths paths}
             {:type ::query}) {} paths])]
    (-> plan
      (with-traversal-tree jbind paths)
      (with-receive-op)
      (with-param-map))))

(defmacro path
  "Create a path query, which is composed of a set of edge
  traversal specifications.

  ; Return the set of paths traversing from the root across two
  ; edges, :foo and :bar.
  (path [:foo :bar])

  ; Bind the node on the target side of the edge(s) labeled :bar to the
  ; path variable b, so it can be referred to later in the query.
  (path [b [:foo :bar]])

  ; Also bind the intermediate node reached by the edge :foo
  (path [f [:foo]
        [b [f :bar]])

  ; By default the edge values you specify will be compared with the :label
  ; property for the associated edge.  You can also use regular expressions,
  ; which will match against (name (:label edge)) if the label is a keyword,
  ; or else as normal if it is a string.
  (path [b [:foo #\"ba[rz]\"]])

  ; Use a map if you need to test on multiple edge properties
  (path [b [{:label :foo :favorite true} :bar]])

  ; Path queries can also be composed, so that one query can continue from
  ; a previous one:
  (let [bar-query (path [b [:foo :bar]])]
    (path [z [bar-query :zap]]))

  "
  [& q]
  (let [[bindings body]
        (cond
          (and (vector? (first q))  ; [:a :b :c] or [node-uuid :b :c]
               (not (vector? (second (first q)))))
          (let [res (gensym 'result)]
            [(vector res (first q)) (list res)])

          (and (vector? (first q))  ; [doc [:apps :docs :doc]]
               (vector? (second (first q))))
          [(first q) (rest q)]

          (symbol? (ffirst q)) [[] q]
          :default (throw
                     (Exception.
                       "Invalid path expression:
                       Missing either a binding or a path operator.")))
        paths (partition 2 bindings)
        path-vars (set (map first paths))
        paths (eval-path-binding paths)]
    `(path* ~paths (quote ~body))))

(defn order-by
  "Sort the results by a specific property value of one of the nodes
  traversed in a path expression."
  ([plan sort-var sort-prop]
   (order-by plan sort-var sort-prop :asc))
  ([plan sort-var sort-prop order]
   (let [{:keys [root ops]} plan
         sort-key (get (:pbind plan) sort-var)
         p-op (plan-op :property
                       :deps [(:root plan)]
                       :args [sort-key [sort-prop]])
         s-op (plan-op :sort
                       :deps [(:id p-op)]
                       :args [sort-key sort-prop order])
         ops (assoc ops
                    (:id p-op) p-op
                    (:id s-op) s-op)]
     (assoc plan
            :root (:id s-op)
            :ops ops))))

(defn query-channel
  "Issue a query to the currently bound graph, and return a channel
  representing the results."
  ([plan]
   (query-channel plan {}))
  ([plan params] (query-channel plan params MAX-QUERY-TIME))
  ([plan params timeout]
   (let [plan (optimize-plan plan)
         tree (query-tree plan timeout)
         param-map (or params {})
         res-chan (get-in (:ops tree) [(:root tree) :out])]
     ;(log/to :stream "query-channel:\n" (with-out-str (print-query plan)))
     (run-query tree param-map)
     res-chan)))

(defn- has-projection?
  "Check whether a query plan contains a project operator."
  [plan]
  (if (first (filter #(= :project (:type %)) (vals (:ops plan))))
    true
    false))

(defn with-result-project
  "If an explicit projection has not been added to the query plan and the
  root operator outputs path maps then by default we project on the final
  element of the path query."
  [plan]
  (if (or (has-projection? plan)
          (:numerical? (root-op plan)))
    plan
    (let [bind-sym (default-project-binding plan)]
            (project plan [bind-sym]))))

(defn query
  "Issue a query to the currently bound graph."
  ([plan]
   (query plan {}))
  ([plan params]
   (query plan params MAX-QUERY-TIME))
  ([plan params timeout]
   (let [plan (with-result-project plan)
         res-chan (query-channel plan params timeout)
         p (promise)]
     (lamina/on-closed res-chan
       (fn [] (deliver p (lamina/channel-seq res-chan))))
     (await-promise p (+ timeout PROMISE-WAIT-TIME)))))

