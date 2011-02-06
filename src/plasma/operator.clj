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

;(log/channel :op :debug)

(def op-branch-map
  {:project   true
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
              :project [(first args)]
              :aggregate [(first args)]
              :join [(second args) (first args)]
              :send [(first args)]
              :select [(first args)]))))

      (fn make-op [op args]
        (assoc op :args args))

      (:root plan))))

(defn sub-query-ops
  "Returns the operator tree from the root out to the end-id,
  and no further."
  [plan end-node-id]
  (let [ops (:ops plan)]
    (loop [loc (operator-deps-zip plan end-node-id)
           sub-query-ops {}]
      (let [op-id (zip/node loc)
            op-node (get ops op-id)]
        (log/to :op (:id op-node) ;(:args op-node)
                "\nop: " (:type op-node))
        (if (or (= end-node-id op-id)
                (zip/end? loc))
          sub-query-ops
          (recur (zip/next loc)
                 (assoc sub-query-ops op-id op-node)))))))

(defn forward-sub-query [q])

(defn build-sub-query [plan end-node-id]
  (assoc plan :ops (sub-query-ops plan end-node-id)))

(comment defn remote-query [root end-id]
  (let [sub-query (build-sub-query root end-id)]
    (forward-sub-query sub-query)))

(defn parameter-op
  "An operator designed to accept a query parameter.  Forwards the
  parameter value to its output channel and then closes it to signify
  that this was the last value."
  [id & [param-name]]
  (let [in (channel)
        out (channel)]
    (receive-all in
      (fn [val]
        (log/to :op "[param] " param-name " => " val)
        (enqueue-and-close out {id val})))
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
  [id left]
  (let [in (channel)
        out (channel)
        left-out (:out left)]
    (siphon left-out out)
    (on-closed left-out #(do
                           (log/to :op "[receive] closed...")
                           (close out)))

    (siphon in out)
    (on-closed in #(do
                     (log/to :op "[receive] closed...")
                     (close out)))

  {:type :receive
   :id id
   :in in
   :out out}))

(defn register-sub-query
  [recv-op ])

(defn send-op
  "A send operator to forward values over the network to a waiting
  receive operator.  Takes a left input operator and a destination network
  channel."
  [id left dest]
  {:pre [(and (channel? (:out left)) (channel? dest))]}
  (log/format :op "[send] left: %s dest: %s" left dest)
  (siphon (:out left) dest)
  {:type :send
   :id id
   :dest dest})

(defn traverse-op
	"Uses the src-key to lookup a node ID from each PT in the in queue.
 For each source node traverse the edges passing the edge-predicate, and put
 target nodes into PTs on out channel."
	[id recv src-key edge-predicate]
  (let [in  (channel)
        out (channel)
        edge-pred-fn (if (keyword? edge-predicate)
                       #(= edge-predicate (:label %1))
                       edge-predicate)]
    (receive-all in
      (fn [pt]
        (when pt
          (let [src-id (get pt src-key)]
            ;(log/to :op "[traverse] in:" pt)
            (log/to :op "[traverse] src: " src-id " - " edge-predicate " -> "
                    (count (get-edges src-id edge-pred-fn)) " edges")
            (if (proxy-node? src-id)
              nil ; TODO: Initiate remote query here... (remote-query recv id)
              (let [edges (get-edges src-id edge-pred-fn)
                    tgts (keys edges)
                    path-tuples (map #(assoc pt id %) tgts)]
                (doseq [path-tuple path-tuples]
                  (log/to :op "[traverse] out: " (get path-tuple id))
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
    (siphon left-out right-in)
    (on-closed left-out #(close right-in))

    (siphon right-out out)
    (on-closed right-out #(do
                            (log/to :op "[join] closed")
                            (close out)))
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
                  (log/format :op 
                              "[select-op] 
                              skey: %s 
                              node-id: %s
                              pt: %s
                              props: %s" select-key
                              node-id
                              pt
                              props)
                (do-predicate props predicate)))
              left-out)
            out)
    (on-closed left-out #(close out))
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
  [id left pt-key prop]
  (log/to :op "[property-op] loader for prop: " prop)
  (let [left-out (:out left)
        out (map* (fn [pt] 
                    (let [node-id (get pt pt-key)
                          node (find-node node-id)
                          val  (get node prop)
                          pt (assoc-in pt [node-id prop] val)]
                      (log/to :op "[property-op] pt out: " pt)
                      pt))
                  left-out)] 
    (on-closed left-out #(close out))
  {:type :property
   :id id
   :left left
   :pt-key pt-key
   :prop prop
   :out out}))

(defn project-op
	"Project will turn a stream of PTs into a stream of node UUIDs.
 Enqueues only the UUID in the PT slot for project-key."
	[id left project-key]
  (let [left-out (:out left)
        out (channel)]
    (siphon (map* #(get % project-key) left-out)
            out)
    (on-closed left-out #(close out))
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

