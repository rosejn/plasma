(ns plasma.query.construct
  (:use [plasma util graph viz]
        [plasma.net url]
        [plasma.query operator helpers expression]
        [lamina core])
  (:require [clojure (zip :as zip)]
            [clojure (set :as set)]
            [logjam.core :as log]
            [jiraph.graph :as jiraph]
            [plasma.query.core :as q])
  (:import [java.util.concurrent TimeUnit TimeoutException]))

(defmacro nodes
  "Convert a node-spec into a builder-query."
  [specs]
  (let [specs (partition 2 specs)
        specs (map (fn [[node-sym props]]
                     (let [props (if (keyword? props)
                                   {:label props}
                                   props)]
                       [`'~node-sym `~props]))
                   specs)
        specs (vec specs)]
    `{:type ::construct-spec
        :nodes ~specs}))

(defmacro edges
  [node-spec edge-specs]
  (let [nodes (:nodes node-spec)
        specs (partition 3 edge-specs)
        specs (map (fn [[src tgt props]]
                     (let [props (if (keyword? props)
                                   {:label props}
                                   props)]
                       `['~src '~tgt ~props]))
                   specs)
        specs (vec specs)]
    `(assoc ~node-spec
            :edges ~specs)))

;; Node Spec
; Composed of pairs, where the left side of the pair is a symbol variable name,
; and the right side is one of:
; * node UUID (an already existing node)
; * map       (a new node's properties)
; * keyword   (a new node's :label property)
; * path expression  (select nodes to link with other nodes)

(defn- make-nodes
  "Convert a node-spec to a node-id map."
  [spec]
  (let [id-mapped (map
                    (fn [[sym props]]
                      [sym (cond
                             (uuid? props) [props]
                             (q/query? props) (map :id (q/query props))
                             (map? props)  [(make-node props)])])
                    (:nodes spec))]
    (into {} id-mapped)))

(defn- make-edges
  [spec node-map]
  (let [edges (map (fn [[src tgt props]]
                     [(node-map src)
                      (node-map tgt)
                      props])
                   (:edges spec))]
    (doseq [[srcs tgts props] edges]
      (doseq [src srcs tgt tgts]
        (make-edge src tgt props)))))

(defn construct
  [spec]
  (when-not jiraph/*graph*
    (throw (Exception. "Cannot run a construct without binding a graph.")))
  (->> spec
    (make-nodes)
    (make-edges spec)))

