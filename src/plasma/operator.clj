(ns plasma.operator
  (:use plasma.core
        [jiraph graph]
        [lamina core])
  (:require [clojure (zip :as zip)]))

(def *debug* true)

(defn operator-deps-zip [root end-op-id]
  (zip/zipper
    (fn branch? [op]
      (case (:type op)
        ::project true
        ::aggregate true
        ::join true
        ::traverse false
        ::parameter false
        ::receive false
        ::send true))

    (fn children [op]
      (let [{:keys [left right id]} op]
            ;left (if (= end-op-id left-id)
            ;       (recv-op left-id)
            ;       (:left op))
            ;right (if (= end-op-id right-id)
            ;       (recv-op right-id)
            ;       (:right op))]
        (cond
;          (= ::receive (:type right)) [right]
          (= end-op-id id) []
          (and left right) [left right]
          :else [left])))

    (fn make-op [op [left right]]
      (assoc op :left left :right right))

    root))

(defn operator-deps
  "Return the operator tree from the root out to the end-id,
  and no further."
  [root end-id]
  (loop [loc (operator-deps-zip root end-id)]
    (let [op-node (zip/node loc)]
      (println "op: " (:type op-node) " id: " (:id op-node))
      (if (zip/end? loc)
        (zip/root loc)
        (recur (zip/next loc))))))

(defn forward-sub-query [q])

(defn build-sub-query [root end-id])

(defn remote-query [root end-id]
  (let [sub-query (build-sub-query root end-id)]
    (forward-sub-query sub-query)))

(defn param-op
  "An operator designed to accept a query parameter.  Forwards the
  parameter value to its output channel followed by nil, to signify
  that this was the last value."
  [id & [param-name]]
  (let [in (channel)
        out (channel)]
    (receive-all in
      (fn [val]
        (enqueue out {id val})
        (enqueue out nil)))
  {:type ::parameter
   :id id
   :in in
   :out out
   :name param-name}))

(defn receive-op
  "A receive operator to accept values from remote query processors."
  [id]
  (let [in (channel)
        out (channel)
        sub-query-count (atom 0)]
  {:type ::receive
   :id id
   :in in
   :out out}))

(defn register-sub-query
  [recv-op ])

(defn send-op
  "A send operator to forward values over the network to a waiting
  receive operator."
  [id left dest]
  (let [left-out (:out left)]
    (receive-all left-out
      (fn [oa] (enqueue dest oa)))
  {:type ::send
   :id id
   :dest dest}))

(defn traverse-op
	"Uses the src-key to lookup a node ID from each OA in the in queue.
 For each source node traverse the edges passing the edge-predicate, and put
 target nodes into OAs on out channel."
	[id recv src-key edge-predicate]
  (let [in  (channel)
        out (channel)
        edge-pred-fn (if (keyword? edge-predicate)
                       #(= edge-predicate (:label %1))
                       edge-predicate)]
    (receive-all in
      (fn [oa]
        ;(println "traverse " (get oa src-key) "-" edge-predicate "-> " (count (get-edges :graph (get oa src-key))))
        (let [uuid (get oa src-key)]
          (if (proxy-node? uuid)
            (remote-query recv id)
            (let [edges (get-edges uuid edge-pred-fn)
                  tgts (keys edges)]
              (doseq [tgt tgts]
                ;(println "traverse - out: " tgt)
                (enqueue out (assoc oa id tgt)))
              (enqueue out nil))))))
    {:type ::traverse
     :id id
     :src-key src-key
     :edge-predicate edge-predicate
     :in in
     :out out}))

(defn join-op
  "For each OA received from the left operator, gets all successive OAs from
  the right operator."
  [id left right]
  (let [left-out  (:out left)
        right-in  (:in right)
        right-out (:out right)
        out				(channel)]
    (receive-all left-out
      (fn [oa]
        ;(println "join - left-out: " oa)
        (when oa
          (enqueue right-in oa))))
    (receive-all right-out
      (fn [oa]
        ;(println "join - right-out: " oa)
				(enqueue out oa)))
    {:type ::join
     :id id
     :left left
     :right right
     :out out}))

(defn aggregate-op
  "Puts all incoming OAs into a buffer queue, and then on receiving nil
  dumps the whole buffer queue into the output queue.

  If an aggregate function, agg-fn, is passed then it will be called and
  passed a seq of all the OAs, and it's result will be sent to the output
  channel."
  [id left & [agg-fn]]
  (let [left-out (:out left)
        buf (channel)
        out (channel)
				agg-fn (or agg-fn identity)]
    (receive-all left-out
      (fn [oa]
				;(println "aggregate: " oa "\nagg-fn: " agg-fn)
        (cond
          (nil? oa) (let [aggregated (agg-fn (channel-seq buf))]
                      (doseq [item aggregated]
                        (enqueue out item)))
          :else (enqueue buf oa))))
    {:type ::aggregate
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
       key-fn  (fn [oa]
                   (get (find-node (get oa sort-key)) sort-prop))
			 sort-fn #(sort-by key-fn comp-fn %)]
	(aggregate-op id left sort-fn)))

(defn min-op
  "Aggregates the input and returns the OA with the minimum value (numerical)
  corresponding to the min-prop property."
  [id left minimum-key min-prop]
  (let [key-fn (fn [oa]
                 (let [node (find-node (get oa minimum-key))
                       pval (get node min-prop)]
                   pval))
        min-fn (fn [arg-seq]
                 [(apply min-key key-fn arg-seq)])]
    (aggregate-op id left min-fn)))

(defn max-op
  "Aggregates the input and returns the OA with the maximum value (numerical)
  corresponding to the max-prop property."
  [id left maximum-key min-prop]
  (let [key-fn (fn [oa]
                 (let [node (find-node (get oa maximum-key))
                       pval (get node min-prop)]
                   pval))
        max-fn (fn [arg-seq]
                 [(apply max-key key-fn arg-seq)])]
    (aggregate-op id left max-fn)))

; TODO: Determine if this really makes sense to include, since it
; returns a value rather than an OA like all the other operators...
(comment defn avg-op
  "Aggregates the input and returns the average value (numerical)
  corresponding to the avg-prop property."
  [left maximum-key min-prop]
  (let [key-fn (fn [oa]
                 (let [node (find-node (get oa maximum-key))
                       pval (get node min-prop)]
                   pval))
        max-fn (fn [arg-seq]
                 [(apply max-key key-fn arg-seq)])]
    (aggregate-op left max-fn)))

(defn do-predicate [node-id predicate]
  (let [node (find-node node-id)
        prop (get node (:property predicate))
        op (ns-resolve *ns* (:operator predicate))
        result (op prop (:value predicate))]
      result))

; Expects a predicate in the form of:
; {:type ::predicate
;  :property :score
;  :value 0.5
;  :operator '>}
(defn select-op
  "Performs a selection by accepting only the OAs for which the
  value for the select-key results in true when passed to the
  selection predicate."
  [id left select-key predicate]
  (let [left-out (:out left)
        out      (channel)]
		(receive-all left-out
		  (fn [oa]
			  (if (nil? oa)
			    (enqueue out oa)
			    (if (do-predicate (get oa select-key) predicate)
				    (enqueue out oa)))))
    {:type ::select
     :id id
     :select-key select-key
     :predicate predicate
     :left left
     :out out}))

(defn project-op
	"Project will turn a stream of OAs into a stream of node UUIDs.
 Enqueues only the UUID in the OA slot for project-key."
	[id left project-key]
  (let [left-out (:out left)
        out (channel)]
    (receive-all left-out
      (fn [oa]
        (when oa
					(enqueue out (get oa project-key)))))
    {:type ::project
     :id id
     :project-key project-key
     :left left
     :out out}))

(defn limit-op
	"Forward only the first N input OAs then nil.

 NOTE: Unlike the other operators this operator will only work once
 after instantiation."
	[id left n]
  (let [left-out (:out left)
        out (take* n left-out)]
    {:type ::limit
     :id id
     :left left
     :out out}))

(defn choose-op
  "Aggregates the input and returns the n OA's chosen at random."
  [id left n]
  (let [choose-fn (fn [arg-seq]
                    (take n (shuffle arg-seq)))]
    (aggregate-op id left choose-fn)))
