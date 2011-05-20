(ns plasma.query.helpers
  (:use [plasma util]))

(defn plan-op
  "Creates a query plan operator node.  Takes an operator type, dependent
  operator ids, and the operator parameters."
  [op & {:keys [deps args]}]
  (when-not (every? uuid? deps)
          (throw (Exception. (str "Invalid operator dependency (must be UUID)."
                                  op " deps: " deps))))
  {:type op
   :id (uuid)
   :deps (vec deps)
   :args (vec args)})

(defn append-root-op
  [{ops :ops :as plan} {id :id :as op}]
  (assoc plan
         :root id
         :ops (assoc ops id op)))

(defn downstream-op-node
  "Find the downstream operator node for the given operator node in the plan."
  [plan op-id]
  (first (filter #(some #{op-id} (:deps %))
                 (vals (:ops plan)))))

(defn replace-input-op
  "Returns a new operator node with the left input replaced with new-left."
  [op old new]
  (let [index (.indexOf (:deps op) old)]
    (assoc-in op [:deps index] new)))

(defn reparent-op
  "Move an operator node in the plan to be the direct downstream operator from a target op."
  [plan op-id tgt-id]
  (let [ops (:ops plan)
        ; create new version of moving op with tgt-id as left input
        op (get ops op-id)
        parent-op-id (first (:deps op))
        new-op (replace-input-op op parent-op-id tgt-id)

        ; create new version of op downstream from tgt with moving op-id as replaced input
        tgt-down (downstream-op-node plan tgt-id)
        tgt-down (replace-input-op tgt-down tgt-id op-id)

        new-ops (assoc ops
                       op-id new-op
                       (:id tgt-down) tgt-down)

        ; create new version of op downstream from moving op old parent as input op
        op-down (downstream-op-node plan op-id)
        new-ops (if (= (:root plan) op-id)
                  new-ops
                  (let [new-op-down (replace-input-op op-down op-id parent-op-id)]
                    (assoc new-ops (:id op-down) new-op-down)))
        plan (if (= (:root plan) op-id)
               (assoc plan :ops new-ops :root parent-op-id)
               (assoc plan :ops new-ops))]
    plan))
