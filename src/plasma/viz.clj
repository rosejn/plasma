(ns plasma.viz
  (:use [plasma core operator])
  (:require [vijual :as vij]
            [clojure (zip :as zip)]))

(defn- tree-vecs* [q root]
  (let [node (get (:ops q) root)
        branch? (get op-branch-map (:type node))
;        _ (println "type: " (:type node)
;                   "\nbranch: " branch?)
        children (if branch?
                   (case (:type node)
                     :join (:args node)
                     (:send
                      :select
                      :project
                      :aggregate) [(first (:args node))])
                   nil)
;        _ (println "children: " children)
        label (case (:type node)
                :traverse (str "tr " (second (:args node)))
                :parameter (str "pr \"" (first (:args node)) "\"")
                (name (:type node)))
        label (str label " [" (apply str (take 4 (drop 5 (:id node)))) "]")]
    (apply vector label (map (partial tree-vecs* q) children))))

(defn- tree-vecs [q]
  [(tree-vecs* q (:root q))])

(defn print-query [plan]
  (vij/draw-tree (tree-vecs plan)))
