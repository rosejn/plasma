(ns plasma.query-test
  (:use [plasma core query viz]
        [clojure test stacktrace]
        [jiraph graph]
        lamina.core
        test-utils)
  (:require [logjam.core :as log]))

(log/console :query)
(log/file :query "query.log")

(deftest path-test-manual
  (let [plan (path [:music :synths :synth])
        tree (query-tree plan)]
    (run-query tree {})
    (is (= #{:kick :bass :snare :hat}
           (set (map #(:label (find-node %))
                     (query (path [:music :synths :synth]))))))))

(deftest path-query-test
  (is (= #{:kick :bass :snare :hat}
         (set (map #(:label (find-node %))
                   (query (path [:music :synths :synth])))))))

(deftest where-query-test
  (is (= #{:kick :bass :snare}
         (set (map #(:label (find-node %))
                   (query (path [s [:music :synths :synth]]
                                (where (> (:score s) 0.3))))))))
  (let [p (path [sesh [:sessions :session]
                 synth [sesh :synth]]
                (where (= (:label synth) :kick))
                sesh)]
    (is (= #{:red-pill :take-six}
           (set (map #(:label (find-node %))
                     (query p)))))))

(defn append-send-node
  [plan]
  (let [{:keys [ops root]} plan
        op (plan-op :send root)
        ops (assoc ops (:id op) op)  ; add to ops
        plan (assoc plan 
                    :type :sub-query
                    :root op
                    :ops ops)]
    plan))

(deftest sub-query-test
  (let [plan (path [:music :synths :synth])
        plan (append-send-node plan)
        res-chan (channel)]
    (sub-query res-chan plan)
    (is (= #{:kick :bass :snare :hat}
           (set (map #(:label (find-node %))
                     (channel-seq res-chan 1000)))))))

(use-fixtures :once test-fixture)

