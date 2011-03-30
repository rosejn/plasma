(ns plasma.operator
  (:use [plasma core util]
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

(defn- close-log
  [op chan]
  (on-closed chan #(log/format :close "[%s] closed" op)))

(defn- flow-log
  [op chan]
  (receive-all (fork chan)
    (fn [pt]
      (log/format :flow "[%s] %s" op pt)))
  (close-log op chan))

(defn plan-op
  "Creates a query plan operator node.  Takes an operator type, dependent
  operator ids, and the operator parameters."
  [op & {:keys [deps args]}]
  {:type op
   :id (uuid)
   :deps (vec deps)
   :args (vec args)})

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
        new-root (first (:deps recv-op))
        new-ops  (sub-query-ops plan new-root end-id)
        ; connect a send op at the root
        send-op (plan-op :send :deps [new-root])
        s-id (:id send-op)

        ; connect a new param op that will start the query at the source of the proxy node
        param-op (plan-op :parameter :args [start-node])
        p-id (:id param-op)

        ; hook the new param node up to the join that feeds the traversal we need to start at
        _ (log/to :sub-query "[build-sub-query] end-id: " end-id "\nnew-ops: " new-ops)
        end-join-op (first (filter #(and (= :join (:type %))
                                         (= end-id (second (:deps %))))
                                   (vals new-ops)))
        _ (log/to :sub-query "[build-sub-query] end-join-op: " end-join-op)
        new-join-op (assoc end-join-op
                           :deps [p-id (second (:deps end-join-op))])
        _ (log/to :sub-query "[build-sub-query] new-join-op: " new-join-op)

        ; and modify the traverse-op's src-key so it uses the new param node's value
        trav-op (get new-ops end-id)
        trav-op (assoc trav-op :args [p-id (second (:args trav-op))])
        _ (log/to :sub-query "[build-sub-query] new-join-op: " new-join-op)

        new-ops (assoc new-ops
                       (:id new-join-op) new-join-op
                       (:id trav-op) trav-op
                       s-id send-op
                       p-id param-op)]
    (assoc plan
           :type :sub-query
           :id (uuid)
           :root (:id send-op)
           :params {start-node p-id}
           :ops new-ops)))

(defn remote-sub-query
  "Generates a sub-query for the given plan, starting at the receive operator
  and ending at the end-id.  The sub-query will begin traversal at the
  src-node-id.  It sends the sub-query to the remote peer, and returns a channel
  that will receive the stream of path-tuple results from the execution of the
  sub-query."
  [plan end-id start-id url]
  (let [sub-query (build-sub-query plan start-id end-id)
        sender (peer-sender url)]
    (log/to :op "sending remote-sub-query: " (:id sub-query))
    ;(log/to :op "[remote-sub-query] sub-query: " sub-query)
    (sender sub-query)))

(defn parameter-op
  "An operator designed to accept a query parameter.  Forwards the
  parameter value to its output channel and then closes it to signify
  that this was the last value."
  [id & [param-name]]
  (let [in (channel)
        out (map* (fn [val] {id val}) in)]
    (on-closed in #(close out))
    (flow-log "parameter" out)
    (close-log "parameter" out)
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
  [id left remotes]
  (let [out (channel)
        left-out (:out left)
        sub-chans (atom [])
        all-closed (fn []
                     (when (and (closed? left-out)
                              (every? closed? @sub-chans))
                       (close out)))]
    ; Wire remote results of sub-queries into the graph
    (receive-all remotes
      (fn [chan]
        (swap! sub-chans conj chan)
        (siphon chan out)
        (on-closed chan all-closed)))

    (siphon left-out out)
    (on-closed left-out all-closed)
    (flow-log "receive" out)

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
    (flow-log "send" out)
    (on-closed left-out
      #(do
         (log/to :op "[send] closed...")
         (close dest))))
  {:type :send
   :id id
   :dest dest})

(defn- predicate-fn
  "Create an edge predicate function, based on the type of predicate object supplied."
  [pred]
  (cond
    (keyword? pred) #(= pred (:label %1))
    (regexp? pred) #(re-find pred %)
    (fn? pred) pred
    :default
    (throw (Exception. (str "Unsupported predicate type: " (type pred))))))

(defn traverse-op
	"Uses the src-key to lookup a node ID from each PT in the in queue.
 For each source node traverse the edges passing the edge-predicate, and put
 target nodes into PTs on out channel."
	[id plan recv-chan src-key edge-predicate]
  (let [in  (channel)
        out (channel)
        edge-pred-fn (predicate-fn edge-predicate)]
    (receive-all in
      (fn [pt]
        (when pt
          (let [src-id (get pt src-key)
                src-node (find-node src-id)]
            (cond
              (proxy-node? src-id)
              (let [proxy (:proxy src-node)]
                    (log/to :flow "[traverse] proxy:" proxy "[" src-id "]")
                    ; Send the remote-sub-query channel to the recv operator
                    (enqueue recv-chan
                             (remote-sub-query plan id src-id proxy)))

              :default
              (let [tgts (keys (get-edges src-id edge-pred-fn))]
                (log/to :flow "[traverse] "
                        src-id " - "
                        edge-predicate " -> " tgts)
                (siphon (apply channel (map #(assoc pt id %) tgts))
                        out)))))))

    (on-closed in #(do
                     (log/to :op "[traverse] closed")
                     (close out)))
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
    (siphon left-out right-in)
    (siphon right-out out)
    (on-closed left-out #(close right-in))
    (on-closed right-out #(close out))
    (flow-log "join" out)
    {:type :join
     :id id
     :left left
     :right right
     :out out}))

(defn aggregate-op
  "Puts all incoming PTs into a buffer queue, and then when the input channel
  is closed dumps the whole buffer queue into the output queue.

  If an aggregate function, agg-fn, is passed then it will be called and
  passed a seq of all the PTs, and it's result will be sent to the output
  channel."
  [id left & [agg-fn]]
  (let [left-out (:out left)
        buf (channel)
        out (channel)
				agg-fn (or agg-fn identity)]
    (siphon left-out buf)
    (flow-log "aggregate in" left-out)
    (on-closed left-out
      (fn []
        (let [aggregated (agg-fn (channel-seq buf))]
          (log/to :op "[aggregate] count: " (count aggregated))
          (doseq [item aggregated]
            (enqueue out item)))
        (close out)))
    (flow-log "aggregate" out)
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
	(aggregate-op id left sort-fn)))

(defn min-op
  "Aggregates the input and returns the PT with the minimum value (numerical)
  corresponding to the min-prop property."
  [id left minimum-key min-prop]
  (let [key-fn (fn [pt]
                 (let [node (find-node (get pt minimum-key))
                       pval (get node min-prop)]
                   pval))
        min-fn (fn [arg-seq]
                 [(apply min-key key-fn arg-seq)])]
    (aggregate-op id left min-fn)))

(defn max-op
  "Aggregates the input and returns the PT with the maximum value (numerical)
  corresponding to the max-prop property."
  [id left maximum-key min-prop]
  (let [key-fn (fn [pt]
                 (let [node (find-node (get pt maximum-key))
                       pval (get node min-prop)]
                   pval))
        max-fn (fn [arg-seq]
                 [(apply max-key key-fn arg-seq)])]
    (aggregate-op id left max-fn)))

; TODO: Determine if this really makes sense to include, since it
; returns a value rather than an PT like all the other operators...
(comment defn avg-op
  "Aggregates the input and returns the average value (numerical)
  corresponding to the avg-prop property."
  [left maximum-key min-prop]
  (let [key-fn (fn [pt]
                 (let [node (find-node (get pt maximum-key))
                       pval (get node min-prop)]
                   pval))
        max-fn (fn [arg-seq]
                 [(apply max-key key-fn arg-seq)])]
    (aggregate-op left max-fn)))

(def PREDICATE-OPS
  {'= =
   '== ==
   'not= not=
   '< <
   '> >
   '<= <=
   '>= >=})

; Expects a predicate in the form of:
; {:type :predicate
;  :property :score
;  :value 0.5
;  :operator '>}
(defn select-op
  "Performs a selection by accepting only the PTs for which the
  value for the select-key results in true when passed to the
  selection predicate."
  [id left select-key predicate]
  (let [left-out (:out left)
        out      (channel)]
    (siphon (filter*
              (fn [pt]
                (let [node-id (get pt select-key)
                      node    (get pt node-id)
                      {:keys [property operator value]} predicate
                      pval (get node property)
                      op (get PREDICATE-OPS operator)
                      result (op pval value)]
                  (log/format :flow "[select] (%s (%s node) %s) => (%s %s %s) => %s"
                              operator property value
                              operator pval value
                              result)
                  result))
              left-out)
            out)
    (on-closed left-out #(close out))
    (flow-log "select" out)
    {:type :select
     :id id
     :select-key select-key
     :predicate predicate
     :left left
     :out out}))

(defn property-op
  "Loads a node property from the database.  Used to pre-load
  properties for operations like select and sort that rely on property
  values already being in the PT map."
  [id left pt-key props]
  (let [left-out (:out left)
        out (map* (fn [pt]
                    (let [node-id  (get pt pt-key)
                          existing (get pt node-id)]
                      ; Only load props from disk of they don't already exist
                      (if (every? #(contains? existing %) props)
                        pt
                        (let [node     (find-node node-id)
                              vals     (select-keys node props)]
                          (assoc pt node-id (merge existing vals))))))
                  left-out)]
    (on-closed left-out #(close out))
    (flow-log "property" out)
  {:type :property
   :id id
   :left left
   :pt-key pt-key
   :props props
   :out out}))

(defn project-op
	"Project will turn a stream of PTs into a stream of either node UUIDs or node
 maps containing properties."
	[id left project-key props?]
  (let [left-out (:out left)
        out (map* (fn [pt]
                    (let [node-id (get pt project-key)]
                      (if props?
                        (get pt node-id)
                        node-id)))
                  left-out)]
    (on-closed left-out #(close out))
    (flow-log "project" out)
    {:type :project
     :id id
     :project-key project-key
     :left left
     :out out}))

(defn limit-op
	"Forward only the first N input PTs then nil.

 NOTE: Unlike the other operators this operator will only work once
 after instantiation."
	[id left n]
  (let [left-out (:out left)
        out (take* n left-out)]
    (flow-log "limit" out)
    {:type :limit
     :id id
     :left left
     :out out}))

(defn choose-op
  "Aggregates the input and returns the n PT's chosen at random."
  [id left n]
  (let [choose-fn (fn [arg-seq]
                    (take n (shuffle arg-seq)))]
    (aggregate-op id left choose-fn)))

(comment defn recurse-op
  "Recursively executes a query, where the output of one iteration is fed as
  input to the next."
  [id left param]
  (siphon left param))

