(ns plasma.operator
  (:use plasma.core
        jiraph
        [aleph core])
  (:require [clojure (zip :as zip)]))

(def *debug* true)

(defn param-op []
  (let [in (channel)
        out (channel)
        id (uuid)]
    (receive-all in 
      (fn [val]
        ;(println "param - out:" {id val} ", nil")
        (enqueue out {id val})
        (enqueue out nil)))
  {:type ::parameter
   :uuid id 
   :val val
   :in in
   :out out}))

(defn traverse-op [src-key edge-predicate]
  (let [in  (channel)
        out (channel)
        edge-pred-fn (if (keyword? edge-predicate)
                       #(= edge-predicate (:label %1))
                       edge-predicate)
        id (uuid)]
    (receive-all in
      (fn [oa]
        ;(println "traverse " (get oa src-key) "-" edge-predicate "-> " (count (get-edges :graph (get oa src-key))))
        (let [edges (vals (get-edges :graph (get oa src-key)))
              tgts  (map :to-id (filter edge-pred-fn edges))]
          (doseq [tgt tgts]
            ;(println "traverse - out: " tgt)
            (enqueue out (assoc oa id tgt))))))
    {:type ::traverse
     :uuid id
     :src-key src-key
     :edge-predicate edge-predicate
     :in in
     :out out}))

(defn join-op [left right]
  (let [left-out (:out left)
        right-in (:in right)
        right-out (:out right)
        out (channel)
        id (uuid)]
    (receive-all left-out
      (fn [oa]
        ;(println "join - left-out: " oa)
        (when oa
          (enqueue right-in oa))))
    (receive-all right-out
      (fn [oa]
        ;(println "join - right-out: " oa)
        (when oa
          (enqueue out oa))))
    {:type ::join
     :uuid id
     :left left
     :right right
     :out out}))

(defn aggregate-op [left]
  (let [left-out (:out left)
        buf (channel)
        out (channel)
        id (uuid)]
    (receive-all left-out
      (fn [oa] 
        (cond 
          (nil? oa) (siphon buf out)
          :else (enqueue buf oa))))
    {:type ::aggregate
     :uuid id
     :left left
     :buffer buf
     :out out}))


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
(defn select-op [left select-key predicate]
  (let [left-out (:out left)
        out      (channel)
        id       (uuid)]
    (receive-all left-out
      (fn [oa] 
        (if (do-predicate (get oa select-key) predicate)
          (enqueue out oa))))
    {:type ::select
     :uuid id
     :select-key select-key
     :predicate predicate
     :left left
     :out out}))

(defn project-op [left project-key]
  (let [left-out (:out left)
        out (channel)
        id (uuid)]
    (receive-all left-out
      (fn [oa] 
        (enqueue out (get oa project-key))))
    {:type ::project
     :uuid id
     :project-key project-key
     :left left
     :out out}))

(defn recv-op [uuid]
  (let [in (channel)
        out (channel)]
  {:type ::receive
   :uuid uuid
   :in in
   :out out}))

(defn send-op [left dest]
  (let [left-out (:out left)]
    (receive-all left-out
      (fn [oa] (enqueue dest oa)))
  {:type ::send
   :uuid (uuid)
   :dest dest}))


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
      (let [left-id  (get-in op [:left :uuid])
            right-id (get-in op [:right :uuid])
            left (if (= end-op-id left-id)
                   (recv-op left-id)
                   (:left op))
            right (if (= end-op-id right-id)
                   (recv-op right-id)
                   (:right op))]
        (cond
          (= ::receive (:type right)) [right]
          (and left right) [left right]
          :else [left])))

    (fn make-op [op [left right]]
      (assoc op :left left :right right))

    root))

(defn operator-deps [root end-id]
  (loop [loc (operator-deps-zip root end-id)]
    (println "op: " (:type (zip/node loc)))
    (if (zip/end? loc)
      (zip/root loc)
      (recur (zip/next loc)))))
