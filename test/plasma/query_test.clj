(ns plasma.query-test
  (:use [plasma util core operator query viz]
        [clojure test stacktrace]
        [jiraph graph]
        lamina.core
        test-utils)
  (:require [logjam.core :as log]))

;(log/console :query)
;(log/file :query "query.log")

(deftest path-test-manual
  (let [plan (path [:music :synths :synth])
        plan (with-result-project plan)
        tree (query-tree plan)]
    (run-query tree {})
    (is (= #{:kick :bass :snare :hat}
           (set (map #(:label (find-node %))
                     (query-results tree)))))))

(deftest path-query-test
  (is (= #{:kick :bass :snare :hat}
         (set (map #(:label (find-node %))
                   (query (path [:music :synths :synth])))))))

(deftest where-query-test
  (let [q (path [s [:music :synths :synth]]
                (where (> (:score s) 0.3)))
        q2 (project q 's :label)]
    (is (every? uuid? (query q)))
    (is (= #{:kick :bass :snare}
           (set (map :label (query q2))))))
  (let [p (-> (path [sesh [:sessions :session]
                 synth [sesh :synth]]
                (where (= (:label synth) :kick)))
            (project 'sesh :label))]
    (is (= #{:red-pill :take-six}
           (set (map :label (query p)))))))

(deftest auto-project-test
  (let [p (path [sesh [:sessions :session]
                 synth [sesh :synth]]
                (where (= (:label synth) :kick)))]
    (is (false? (has-projection? p)))
    (is (true? (has-projection? (project p 'sesh :label))))))

(deftest project-test
  (let [q (-> (path [synth [:music :synths :synth]])
            (project 'synth :label))
        q2 (-> (path [synth [:music :synths :synth]])
             (project 'synth))]
    (is (= #{:kick :bass :snare :hat}
           (set (map :label (query q)))))
    (is (= #{:kick :bass :snare :hat}
           (set (map (comp :label find-node) (query q2)))))))

(deftest count-test
  (let [q (count* (limit (path [synth [:music :synths :synth]])
                 2))]
    (is (= 2 (first (query q))))))

(deftest limit-test
  (let [q (limit (path [synth [:music :synths :synth]])
                 2)]
    (is (= 2 (count (query q))))))

(deftest choose-test
  (let [q (choose (path [synth [:music :synths :synth]])
                 2)]
    (is (= 2 (count (query q))))))

(defn append-send-node
  [plan]
  (let [{:keys [ops root]} plan
        op (plan-op :send
                    :deps [root])
        ops (assoc ops (:id op) op)  ; add to ops
        plan (assoc plan
                    :type :sub-query
                    :root op
                    :ops ops)]
    plan))

(deftest sub-query-test
  (let [plan (path [synth [:music :synths :synth]])
        plan (project plan 'synth :label)
        res-chan (channel)]
    (sub-query res-chan plan)
    (is (= #{:kick :bass :snare :hat}
           (set (map :label
                     (channel-seq res-chan 1000)))))))

(deftest optimizer-test
  (let [plan (-> (path [sesh [:sessions :session]
                        synth [sesh :synth]]
                   (where (= (:label synth) :kick)))
               (project 'sesh :label))
        tree (query-tree plan)
        optimized (optimize-plan plan)
        opt-tree (query-tree optimized)]
    ;(print-query plan)
    ;(print-query optimized)
    (run-query tree {})
    (run-query opt-tree {})
    (is (= (doall (query-results tree))
           (doall (query-results opt-tree))))))

(comment deftest recurse-test
  (recurse (path [:net :peer])
           :count 10))

(use-fixtures :each test-fixture)

