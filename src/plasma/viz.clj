(ns plasma.viz
  (:use [plasma core])
  (:require [vijual :as vij]
            [clojure (zip :as zip)]))

(def op-branch-map
  {:plasma.operator/project   true
   :plasma.operator/aggregate true
   :plasma.operator/join      true
   :plasma.operator/traverse  false
   :plasma.operator/parameter false
   :plasma.operator/receive   false
   :plasma.operator/send      true
   :plasma.operator/select    true})

(defn tree-vecs* [q root]
  (let [node (get (:ops q) root)
        branch? (get op-branch-map (:type node))
;        _ (println "type: " (:type node)
;                   "\nbranch: " branch?)
        children (if branch?
                   (filter uuid? (:args node))
                   nil)
        label (case (:type node)
                :plasma.operator/traverse (str "tr " (second (:args node)))
                :plasma.operator/parameter (str "pr \"" (first (:args node)) "\"")
                (name (:type node)))
        label (str label " [" (apply str (take 4 (drop 5 (:id node)))) "]")]
    (apply vector label (map (partial tree-vecs* q) children))))

(defn tree-vecs [q]
  [(tree-vecs* q (:root q))])

(defn query-tree [q]
  (vij/draw-tree (tree-vecs q)))
