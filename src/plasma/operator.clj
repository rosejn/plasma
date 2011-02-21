(ns plasma.operator
  (:use plasma.core
        [jiraph graph]
        [lamina core])
  (:require [clojure (zip :as zip)]
            [logjam.core :as log]))

; Query operators work on path tuples (PTs), which are maps representing a
; traversal from one start node to one end node, with zero or more intermediate
; nodes in between and no branching.  Each slot in a PT map has as its key the
; ID of the operator that placed it into the PT and as its value the UUID of
; a graph node.

(log/channel :op :debug)
(log/channel :flow :op) ; to log values flowing through the operator graph

(def op-branch-map
  {:project   true
   :property  true
   :aggregate true
   :join      true
   :traverse  false
   :parameter false
   :receive   true
   :send      true
   :select    true})

(defn operator-deps-zip [plan end-op-id]
  (let [ops (:ops plan)]
    (zip/zipper
      (fn branch? [op-id]
        (if (= end-op-id op-id)
          false
          (get op-branch-map (:type (get ops op-id)))))

      (fn children [op-id]
        (if (= end-op-id op-id)
          []
          (let [op (get ops op-id)
                {:keys [type args]} op]
            (case type
              :property [(first args)]
              :project [(first args)]
              :aggregate [(first args)]
              :join [(second args) (first args)]
              :send [(first args)]
              :receive [(first args)]
              :select [(first args)]))))

      (fn make-op [op args]
        (assoc op :args args))

      (:root plan))))

(defn sub-query-ops
  "Returns the operator tree from the root out to the end-id,
  and no further."
  [plan end-node-id]
  (log/to :op "[sub-query-ops] start-plan: " plan)
  (let [ops (:ops plan)]
    (loop [loc (operator-deps-zip plan end-node-id)
           sub-query-ops {}]
      (let [op-id (zip/node loc)
            op-node (get ops op-id)]
        (log/to :op "[sub-query-ops] op-node: " op-node)
        (if (or (= end-node-id op-id)
                (zip/end? loc))
          (assoc sub-query-ops op-id op-node)
          (recur (zip/next loc)
                 (assoc sub-query-ops op-id op-node)))))))

(defn build-sub-query 
  [plan end-op-id src-node-id]
  (let [recv-op  (first (filter #(= :receive (:type %)) (vals (:ops plan))))
        new-root (first (:args recv-op))
        new-plan (assoc plan :root new-root)
        new-ops  (sub-query-ops new-plan end-op-id)
        ; connect a send op at the root
        s-id (uuid)
        send-op {:type :send :id s-id :args [new-root]}

        ; connect a new param op that will start the query at the source of the proxy node
        p-id (uuid)
        param-op {:type :parameter :id p-id :args [src-node-id]}

        ; hook the new param node up to the join that feeds the traversal we need to start at
        _ (log/to :op "[build-sub-query] end-op-id: " end-op-id "\nnew-ops: " new-ops)
        end-join-op (first (filter #(and (= :join (:type %))
                                         (= end-op-id (second (:args %)))) 
                                   (vals new-ops)))
        _ (log/to :op "[build-sub-query] end-join-op: " end-join-op)
        new-join-op (assoc end-join-op
                           :args [p-id (second (:args end-join-op))])
        _ (log/to :op "[build-sub-query] new-join-op: " new-join-op)

        ; and modify the traverse-op's src-key so it uses the new param node's value
        trav-op (get new-ops end-op-id)
        trav-op (assoc trav-op :args [p-id (second (:args trav-op))])
        _ (log/to :op "[build-sub-query] new-join-op: " new-join-op)

        new-ops (assoc new-ops 
                       (:id new-join-op) new-join-op
                       (:id trav-op) trav-op
                       s-id send-op
                       p-id param-op)]
    (assoc new-plan
           :root (:id send-op)
           :params {src-node-id p-id}
           :type :sub-query
           :ops new-ops)))

(defn remote-sub-query 
  "Generates a sub-query for the given plan, starting at the receive operator
  and ending at the end-id.  The sub-query will begin traversal at the
  src-node-id.  It sends the sub-query to the remote peer, and returns a channel
  that will receive the stream of path-tuple results from the execution of the
  sub-query."
  [plan end-id src-node-id url]
  (let [sub-query (build-sub-query plan end-id src-node-id)
        sender (peer-sender url)]
    (log/to :op "[remote-sub-query] sub-query: " sub-query)
    (sender sub-query)))

(defn- flow-log 
  [op chan]
  (receive-all (fork chan)
    (fn [pt]
      (log/format :flow "[%s] out: %s" op pt)))) 

(defn parameter-op
  "An operator designed to accept a query parameter.  Forwards the
  parameter value to its output channel and then closes it to signify
  that this was the last value."
  [id & [param-name]]
  (let [in (channel)
        out (map* (fn [val] {id val}) in)]
    (flow-log "parameter" out)
    (on-closed in #(do
                     (log/to :flow "[parameter] closed")
                     (close out)))
  {:type :parameter
   :id id
   :in in
   :out out
   :name param-name}))

; TODO: Need to register remote queries so we can close only after all
; results have been received or a timeout has occurred.
(defn receive-op
  "A receive operator to merge values from local query processing and
  remote query results."
  [id left in]
  (let [out (channel)
        left-out (:out left)]
    (siphon left-out out)
    (siphon in out)
    (flow-log "receive" out)

    ;(on-closed left-out #(do
    ;                       (log/to :op "[receive] closed...")
    ;                       (close out)))

    (on-closed in #(do
                     (log/to :flow "[receive] closed...")
                     (close out)))

  {:type :receive
   :id id
   :in in
   :out out}))

(defn send-op
  "A send operator to forward values over the network to a waiting
  receive operator.  Takes a left input operator and a destination network
  channel."
  [id left dest]
  {:pre [(and (channel? (:out left)) (channel? dest))]}
  (log/format :op "[send] left: %s dest: %s" left dest)
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

(defn recv-from
  "Forward results from a sub-query result channel to a receive channel.
  Could just use siphon, but it's nice to have logging..."
  [res-chan recv-chan]
  (siphon res-chan recv-chan)
  (flow-log "recv-from" recv-chan))

(defn traverse-op
	"Uses the src-key to lookup a node ID from each PT in the in queue.
 For each source node traverse the edges passing the edge-predicate, and put
 target nodes into PTs on out channel."
	[id plan recv-chan src-key edge-predicate]
  (let [in  (channel)
        out (channel)
        edge-pred-fn (if (keyword? edge-predicate)
                       #(= edge-predicate (:label %1))
                       edge-predicate)]
    (receive-all in
      (fn [pt]
        (when pt
          (let [src-id (get pt src-key)]
            (log/to :flow "[traverse] in:" pt)
            (log/to :flow "[traverse] src: " src-id " - " edge-predicate " -> "
                    (count (get-edges src-id edge-pred-fn)) " edges")
            (if (proxy-node? src-id)
              (do
                (log/to :flow "[traverse] proxy-node:" (:proxy (find-node src-id)))
                (recv-from 
                  (remote-sub-query plan id src-id (:proxy (find-node src-id)))
                  recv-chan))
              (let [edges (get-edges src-id edge-pred-fn)
                    tgts (keys edges)
                    path-tuples (map #(assoc pt id %) tgts)]
                (doseq [path-tuple path-tuples]
                  (log/to :flow "[traverse] out: " (get path-tuple id))
                  (enqueue out path-tuple))))))))

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
    (log/to :op-b "[join] left: " left " right: " right)
    (siphon left-out right-in)
    (on-closed left-out #(close right-in))

    (siphon right-out out)
    (on-closed right-out #(do
                            (log/to :op "[join] closed")
                            (close out)))
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
    (on-closed left-out
      (fn []
        (let [aggregated (agg-fn (channel-seq buf))]
          (log/to :op "[aggregate] count: " (count aggregated))
          (doseq [item aggregated]
            (enqueue out item)))
        (log/to :op "[aggregate] closed")
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

(defn- do-predicate [props predicate]
  (let [prop (get props (:property predicate))
        op (ns-resolve *ns* (:operator predicate))
        result (op prop (:value predicate))]
    (log/to :op "[do-predicate] prop: " prop " op: " op "result: " result)
      result))

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
                      props (get pt node-id)]
                  (log/format :flow
                              "[select-op] skey: %s\n\tnode-id: %s\n\tpt: %s\n\tprops: %s\n\tpredicate: %s" 
                              select-key node-id pt props predicate)
                (do-predicate props predicate)))
              left-out)
            out)
    (on-closed left-out #(close out))
;    (flow-log "select" out)
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
  (log/to :op "[property-op] loader for props: " props)
  (let [left-out (:out left)
        out (map* (fn [pt]
                    (log/to :flow "[property] pt: " pt)
                    (let [node-id (get pt pt-key)
                          node (find-node node-id)
                          _ (log/to :flow "[property] node: " node
                                    " props: " props)
                          vals (select-keys node props)
                          existing (get pt node-id)
                          pt  (assoc pt node-id (merge existing vals))]
                      (log/to :flow "[property] pt out: " pt)
                      pt))
                  left-out)]
    (on-closed left-out #(do
                           (log/to :flow "[property] closed...")
                           (close out)))
  {:type :property
   :id id
   :left left
   :pt-key pt-key
   :props props
   :out out}))

(defn project-op
	"Project will turn a stream of PTs into a stream of either node UUIDs or node maps containing properties."
	[id left project-key props?]
  (let [left-out (:out left)
        out (map* (fn [pt]
                    (log/to :flow "[project] proj-key: " project-key "\npt: " pt)
                    (let [node-id (get pt project-key)]
                      (if props?
                        (get pt node-id)
                        node-id))) 
                  left-out)]
    (on-closed left-out #(do
                           (log/to :flow "[project] closed...")
                           (close out)))
    
    (comment receive-all left-out
      (fn [pt]
        (when pt
          (log/to :op "[project] out: " (get pt project-key))
					(enqueue out (get pt project-key)))
        (when (nil? pt)
          (log/to :op "[project] got nil!!!!!!!!!!!!!")
          (close out))))
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

