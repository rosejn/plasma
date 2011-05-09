(ns plasma.query.helpers)

(defn append-root-op
  [{ops :ops :as plan} {id :id :as op}]
  (assoc plan
         :root id
         :ops (assoc ops id op)))

