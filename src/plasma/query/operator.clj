(ns plasma.query.operator
  (:use [plasma graph util]
        [plasma.query expression helpers]
        clojure.contrib.generic.math-functions
        [jiraph graph]
        [lamina core])
  (:require [clojure (zip :as zip)]
            [logjam.core :as log]))

; Query operators work on path tuples (PTs), which are maps representing a
; traversal from one start node to one end node, with zero or more intermediate
; nodes in between and no branching.  Each slot in a PT map has as its key the
; ID of the operator that placed it into the PT and as its value the UUID of
; a graph node.

;(log/channel :op :debug)
(log/channel :flow :debug)    ; log values flowing through the operator graph
(log/channel :close :flow) ; log operators closing their output channels

(defn- trim-pt
  [pt]
  (if (map? pt)
    (let [tr #(if (uuid? %)
                (trim-id %)
                %)]
      (zipmap (map tr (keys pt))
              (map tr (vals pt))))
    pt))

(defn- flow-log
  [op id chan]
  (let [id (trim-id id)]
    (receive-all (fork chan)
                 (fn [pt]
                   (log/format :flow "[%s - %s] %s" op id (trim-pt pt))))
    (on-closed chan #(log/format :close "[%s - %s] closed" op id))))

(defn operator-deps-zip [plan start-id end-id]
  (let [ops (:ops plan)]
    (zip/zipper
      (fn branch? [op-id]
        (and (not= end-id op-id)
             (not (empty? (get-in ops [op-id :deps])))))

      (fn children [op-id]
        (get-in ops [op-id :deps]))

      (fn make-op [op-id deps]
        (assoc (get ops op-id) :deps deps))

      start-id)))

(defn sub-query-ops
  "Returns the operator tree from the root out to the end-id,
  and no further."
  [plan start-id end-id]
  ;(log/to :op "[sub-query-ops] start-plan: " plan)
  (let [ops (:ops plan)]
    (loop [loc (operator-deps-zip plan start-id end-id)
           sub-query-ops {}]
      (let [op-id (zip/node loc)
            op-node (get ops op-id)]
        ;(log/to :sub-query "[sub-query-ops] op-node: " op-node)
        (if (zip/end? loc)
          (assoc sub-query-ops op-id op-node)
          (recur (zip/next loc)
                 (assoc sub-query-ops op-id op-node)))))))

(defn build-sub-query
  "Generates a sub-query plan from the "
  [plan start-node end-id]
  (let [recv-op  (first (filter #(= :receive (:type %)) (vals (:ops plan))))
        ;new-root (first (:deps recv-op))
        new-root (:id recv-op)
        new-ops  (sub-query-ops plan new-root end-id)

        ; connect a new param op that will start the query at the source of the proxy node
        param-op (plan-op :parameter :args [start-node])
        p-id (:id param-op)

        ; hook the new param node up to the join that feeds the traversal we need to start at
        #_ (log/to :sub-query "[build-sub-query] end-id: " end-id "\nnew-ops: " new-ops)
        end-join-op (first (filter #(and (= :join (:type %))
                                         (= end-id (second (:deps %))))
                                   (vals new-ops)))
        #_ (log/to :sub-query "[build-sub-query] end-join-op: " end-join-op)
        new-join-op (assoc end-join-op
                           :deps [p-id (second (:deps end-join-op))])
        #_ (log/to :sub-query "[build-sub-query] new-join-op: " new-join-op)

        ; and modify the traverse-op's src-key so it uses the new param node's value
        trav-op (get new-ops end-id)
        trav-op (assoc trav-op :args [p-id (second (:args trav-op))])
        #_ (log/to :sub-query "[build-sub-query] new-join-op: " new-join-op)

        new-ops (assoc new-ops
                       (:id new-join-op) new-join-op
                       (:id trav-op) trav-op
                       p-id param-op)]
    (log/to :sub-query "[build-sub-query]:\n"
            (with-out-str
              (doseq [[id op] new-ops]
                (println (trim-id id)  ":" (:type op) (:args op)))))
    (assoc plan
           :root new-root
           :params {start-node p-id}
           :ops new-ops)))

(defn remote-sub-query
  "Generates a sub-query for the given plan, starting at the receive operator
  and ending at the end-id.  The sub-query will begin traversal at the
  src-node-id.  It sends the sub-query to the remote peer, and returns a channel
  that will receive the stream of path-tuple results from the execution of the
  sub-query."
  [plan end-op-id start-node-id url]
  (let [sub-query (build-sub-query plan start-node-id end-op-id)
        sender (peer-sender url)]
    (log/to :flow "[remote-sub-query]" (:id sub-query))
    ;(log/to :op "[remote-sub-query] sub-query: " sub-query)
    (sender sub-query)))

(defn parameter-op
  "An operator designed to accept a query parameter.  Forwards the
  parameter value to its output channel and then closes it to signify
  that this was the last value."
  [id & [param-name]]
  (let [in (channel)
        out (map* (fn [v] {id v}) in)]
    (on-drained in #(close out))
    (flow-log "parameter" id out)
  {:type :parameter
   :id id
   :in in
   :out out
   :name param-name}))

(defn receive-op
  "A receive operator to merge values from local query processing and
  remote query results.

  Network receive channels are sent to the remotes channel so we can wire them into
  the running query.
  "
  [id left remotes timeout]
  (let [out (channel)
        left-out (:out left)
        sub-chans (atom [])
        all-closed (fn []
                     (if-not (empty? @sub-chans)
                       (log/format :close "[receive] sub-chan closed (%d/%d open)"
                                   (count (filter #(not (closed? %)) @sub-chans))
                                   (count @sub-chans)))
                     (when (and (closed? left-out)
                              (every? closed? @sub-chans))
                       (close out)))]
    ; Wire remote results of sub-queries into the graph
    (receive-all remotes
      (fn [chan]
        (swap! sub-chans conj chan)
        (siphon chan out)
        (channel-timeout chan timeout)
        (on-closed chan #(log/to :flow "remote-channel closed"))
        (on-drained chan #(do (log/to :flow "remote-channel drained")
                            (all-closed)))))

    (siphon left-out out)
    (on-drained left-out all-closed)
    (flow-log "receive" id out)

  {:type :receive
   :id id
   :in remotes
   :out out}))

(defn send-op
  "A send operator to forward values over the network to a waiting
  receive operator.  Takes a left input operator and a destination network
  channel."
  [id left dest]
  {:pre [(and (channel? (:out left)) (channel? dest))]}
  ;(log/format :op "[send] left: %s dest: %s" left dest)
  (let [left-out (:out left)
        out (channel)]
    (siphon left-out out)
    (siphon out dest)
    (flow-log "send" id out)
    (on-drained left-out
      #(do
         (log/to :close "[send] closed")
         (close dest))))
  {:type :send
   :id id
   :dest dest})

(defn- predicate-fn
  "Create an edge predicate function, based on the type of predicate object supplied."
  [pred]
  (cond
    (keyword? pred) #(= pred (:label %))
    (regexp? pred) #(re-find pred (let [lbl (:label %)]
                                    (if (keyword? lbl)
                                      (name lbl)
                                      lbl)))
    (fn? pred) pred
    :default
    (throw (Exception. (str "Unsupported predicate type: " (type pred))))))

(defn- visit [s id]
  (dosync
    (ensure s)
    (if (@s id)
      false
      (alter s conj id)))
  true)

; TODO: Using visitation causes problems with some queries (i.e. example/products-with-components-from), so maybe it's better to have the duplicate results after all...
(defn traverse-op
	"Uses the src-key to lookup a node ID from each PT in the in queue.
 For each source node traverse the edges passing the edge-predicate, and put
 target nodes into PTs on out channel."
	[id plan recv-chan src-key edge-predicate]
  (let [in  (channel)
        out (channel)
        visited (ref #{})
        edge-pred-fn (predicate-fn edge-predicate)]
    (receive-all in
      (fn [pt]
        #_(log/format :flow "[traverse - %s] pt: %s" (trim-id id) pt)
        (when pt
          (let [src-id (get pt src-key)]
            (when (visit visited src-id)
              (let [src-node (find-node src-id)]
                (cond
                  (proxy-node? src-id)
                  (let [proxy-url (:proxy src-node)
                        res-chan (remote-sub-query plan id src-id proxy-url)
                        combined-chan (map* #(merge pt %) res-chan)]
                    (log/format :flow "[traverse - %s] proxy-node => id: %s proxy: %s "
                                (trim-id id)
                                (trim-id src-id) proxy-url)
                    ; Send the remote-sub-query channel to the recv operator
                    (enqueue recv-chan combined-chan)
                    (on-closed res-chan #(close combined-chan)))

                  :default
                  (let [tgts (keys (get-edges src-id edge-pred-fn))
                        pts (map #(assoc pt id %) tgts)]
                    (log/format :flow "[traverse] %s - %s -> [%s]"
                                src-id edge-predicate
                                (apply str (interpose " " (map trim-id tgts))))
                    (apply enqueue out pts)))))))
        (if (drained? in) (close out))))

    (flow-log "traverse" id out)
    {:type :traverse
     :id id
     :src-key src-key
     :edge-predicate edge-predicate
     :in in
     :out out}))

(defn join-op
  "For each PT received from the left operator, gets all successive PTs from
  the right operator."
  [id left right]
  (let [left-out  (:out left)
        right-in  (:in right)
        right-out (:out right)
        out				(channel)]
    #_(log/format :flow "[join - %s] left: %s right: %s" (trim-id id)
                (trim-id (:id left)) (trim-id (:id right)))
    (siphon left-out right-in)
    (siphon right-out out)
    (on-drained left-out #(close right-in))
    (on-drained right-out #(close out))
    (flow-log "join" id out)
    {:type :join
     :id id
     :left left
     :right right
     :out out}))

(defn id-map-op
  "Used for nested sub-queries.  Just maps PTs to their :id, which will
  serve as the src nodes for the following query."
  [id left]
  (let [left-out (:out left)
        out (map* (fn [pt] {id (:id pt)}) left-out)]
    (log/format :flow "[id-map - %s] left: %s" (trim-id id)
                (trim-id (:id left)))
    (flow-log "id-map" id left-out)
    (on-drained left-out #(close out))
    {:type :id-map
     :id id
     :left left
     :out out}))

(defn distinct-op
  [id left d-key d-props]
  (let [left-out (:out left)
        mem (ref {})
        out (filter* (fn [pt]
                       (let [d-id (get pt d-key)
                             props (if (empty? d-props)
                                     []
                                     (select-keys (get pt d-id) d-props))
                             mem-key [d-id props]
                             ;_ (log/format :flow "[distinct - %s] mem-key: %s" (trim-id id) mem-key)
                         res (dosync
                           (if (get @mem mem-key)
                             false
                             (alter mem assoc mem-key true)))]
                         (log/format :flow "[distinct - %s] res: %s" (trim-id id) res)
                         res))
                     left-out)]
    (flow-log "distinct" id left-out)
    (on-drained left-out #(close out))
    {:type :id-map
     :id id
     :left left
     :out out}))

(defn aggregate-op
  "Puts all incoming PTs into a buffer queue, and then when the input channel
  is closed dumps the whole buffer queue into the output queue.

  If an aggregate function is passed then the fn will be called and
  passed a seq of all the PTs, and it's result will be sent to the output
  channel.  The op-name will be used for log messages."
  [id left & [agg-fn op-name]]
  (let [left-out (:out left)
        buf (channel)
        out (channel)
				agg-fn (or agg-fn identity)]
    (siphon left-out buf)
    (flow-log op-name id left-out)
    (on-drained left-out
      (fn []
        (let [aggregated (agg-fn (channel-seq buf))]
          (doseq [item aggregated]
            (enqueue out item)))
        (close out)))
    (flow-log op-name id out)
    {:type :aggregate
     :id id
     :left left
     :buffer buf
     :out out}))

(defn sort-op
	"Aggregates all input and then sorts by sort-prop.  Specify the
 sort order using either :asc or :desc for ascending or descending."
	[id left sort-key sort-prop & [order]]
	(let [order (or order :asc)
       comp-fn (if (= :desc order)
	  							 #(* -1 (compare %1 %2))
	  							 compare)
       key-fn  (fn [pt]
                 (let [node-id (get pt sort-key)
                       props (get pt node-id)
                       val   (get props sort-prop)]
                   val))
			 sort-fn #(sort-by key-fn comp-fn %)]
	(aggregate-op id left sort-fn "sort")))

(defn min-op
  "Aggregates the input and returns the PT with the minimum value (numerical)
  corresponding to the min-prop property."
  [id left minimum-key min-prop]
  (let [key-fn (fn [pt]
                 (let [node (find-node (get pt minimum-key))
                       pval (get node min-prop)]
                   pval))
        min-fn (fn [arg-seq]
                 (if (empty? arg-seq)
                   []
                   [(apply min-key key-fn arg-seq)]))]
    (aggregate-op id left min-fn "min")))

(defn max-op
  "Aggregates the input and returns the PT with the maximum value (numerical)
  corresponding to the max-prop property."
  [id left maximum-key min-prop]
  (let [key-fn (fn [pt]
                 (let [node (find-node (get pt maximum-key))
                       pval (get node min-prop)]
                   pval))
        max-fn (fn [arg-seq]
                 (if (empty? arg-seq)
                   []
                   [(apply max-key key-fn arg-seq)]))]
    (aggregate-op id left max-fn "max")))

(defn group-by-op
  "Aggregates the input and returns seqs of results grouped by a specific matching property."
  [id left group-key group-prop]
  (let [key-fn (fn [pt]
                 (let [node (->> group-key (get pt) (get pt))]
                   (get node group-prop)))
        group-fn (fn [pts]
                   (let [res (vals (group-by key-fn pts))]
                     (log/to :flow "[group-by] res: " res)
                     res))]
    (aggregate-op id left group-fn "group-by")))

(defn property-op
  "Loads a node property from the database.  Used to pre-load
  properties for operations like select and sort that rely on property
  values already being in the PT map."
  [id left pt-key props]
  (log/format :op "[property] id: %s left: %s" id (:id left))
  (let [left-out (:out left)
        out (map* (fn [pt]
                    (let [node-id  (get pt pt-key)
                          existing (get pt node-id)]
                      ; Only load props from disk of they don't already exist
                      #_(log/format :flow "[property] pt-key: %s props: %s\npt: %s"
                                  pt-key props pt)
                      (if (every? #(contains? existing %) props)
                        pt
                        (let [node     (find-node node-id)
                              vals     (select-keys node props)]
                          #_(log/format :flow "[property] node: %s" node)
                          (assoc pt node-id (merge existing vals))))))
                  left-out)]
    (on-drained left-out #(close out))
    (flow-log "property" id out)
  {:type :property
   :id id
   :left left
   :pt-key pt-key
   :props props
   :out out}))

(defn expression-op
  "Executes an expression (function call) on bound query variables."
  [id left op args]
  (let [left-out (:out left)
        out (map* (fn [pt]
                    (let [args (map #(or (get pt %) %) args)
                          op-fn (resolve op)
                          result (apply op args)]
                      (assoc pt id result)))
                  left-out)]
  {:type :expression
   :id id
   :left left
   :op op
   :args args
   :out out}))

(defn filter-op
  "Filter based on a computed boolean expression."
  [id left pvars expr]
  (log/format :op "[filter-op] expr: %s\nwith pvars: %s" expr pvars)
  (let [expr-form (read-string expr)
        _ (log/to :op "[filter-op] expr-form: " expr-form)
        left-out (:out left)
        out (filter* (fn [pt]
                       (let [bind-vars
                             (map (fn [{:keys [name bind-key property]}]
                                    (let [node-id (get pt bind-key)
                                          pval (get-in pt [node-id property])]
                                      [name pval]))
                                  pvars)]
                         (eval-with-vars bind-vars expr-form)))
                     left-out)]
    (on-closed left-out #(close out))
    (flow-log "filter" id out)
    {:type :filter
     :id id
     :left left
     :pvars pvars
     :expr expr
     :out out}))

(defn project-op
	"Project will turn a stream of PTs into a stream of either node UUIDs or node
 maps containing properties.  The projections are of the form:

   [project-key [prop-1 p1-label] [prop-2 p2-label] [prop-3 p3-label]
 "
	[id left projections]
  (log/format :op "[project - %s] projections: %s"
              (trim-id id) (seq projections))
  (let [left-out (:out left)
        out (map* (fn [pt]
                    (when pt
                      (reduce
                        (fn [result [project-key & props]]
                          (let [pid (get pt project-key)
                                m (assoc (get pt pid) :id pid)
                                prop-keys (map first props)
                                prop-vals (select-keys m prop-keys)
                                labeled-props (into {}
                                                    (map (fn [[k l]]
                                                           [l (get prop-vals k)])
                                                         props))
                                res (merge result labeled-props)]
                          (log/format :flow "[project - %s] %s result: %s"
                                      (trim-id id)
                                      (str project-key "->" props)
                                      res)
                            res))
                        {} projections)))
                  left-out)]
    (on-closed left-out #(close out))
    (flow-log "project" id out)
    {:type :project
     :id id
     :projections projections
     :left left
     :out out}))

(defn limit-op
	"Forward only the first N input PTs then nil.

 NOTE: Unlike the other operators this operator will only work once
 after instantiation."
	[id left n]
  (let [left-out (:out left)
        out (take* n left-out)]
    (on-drained left-out #(close out))
    (flow-log "limit" id out)
    {:type :limit
     :id id
     :left left
     :out out}))

(defn choose-op
  "Aggregates the input and returns the n PT's chosen at random."
  [id left n]
  (let [choose-fn (fn [arg-seq]
                    (take n (shuffle arg-seq)))]
    (aggregate-op id left choose-fn "choose")))

(defn count-op
  "Outputs the total count of its aggregated input."
  [id left]
  (let [count-fn (fn [arg-seq] [(count arg-seq)])]
    (aggregate-op id left count-fn "count")))

(defn average-op
  "Aggregates the input and returns the average value (numerical)
  corresponding to the avg-prop property."
  [id left avg-key avg-prop]
  (let [key-fn (fn [pt]
                 (let [node (find-node (get pt avg-key))
                       pval (get node avg-prop)]
                   pval))
        avg-fn (fn [arg-seq]
                 [(average (map key-fn arg-seq))])]
    (aggregate-op id left avg-fn "average")))

