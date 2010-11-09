(ns plasma.query
  (:use [plasma core]))

(defprotocol Operator
  "A graph query operator."
  (next-res [this] "return the next result map or nil."))

(defrecord Traverse [uuid src pred]
  "src is either a constant (start node) or another operator that will give us a result map when calling next-res."
  Operator
  (next-res [this] nil))

(defrecord Join [uuid left right]
  Operator
  (next-res [this] nil))

(defn where-form [body]
  (let [where? #(and (list? %) (= 'where (first %)))
        where (first (filter where? body))]
    where))

(defn path->traversal 
  ([path-seq] (path->traversal (Traverse. (uuid) 
                                          (first path-seq)
                                          (second path-seq))
  ([ops path-seq]


(defn path* [q]
  (let [[bindings body]
        (cond
          (and (vector? (first q))  ; (query [:a :b :c])
               (keyword? (ffirst q))) (let [res (gensym 'result)]
                                        [(vector res (first q)) (list res)])

          (and (vector? (first q))  ; (query [foo [:a :b :c]])
               (vector? (second (first q)))) [(first q) (rest q)]

          (symbol? (ffirst q)) [[] q]
          :default (throw
                     (Exception.
                       "Invalid query expression:
                       Missing either a binding or a query operator.")))
        where (where-form body)]
    (partition 2 bindings)))

(defmacro path [& args]
  `(path* (quote ~args)))

