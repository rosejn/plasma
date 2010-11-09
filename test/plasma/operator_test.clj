(ns plasma.operator-test
  (:use [plasma core operator]
        [clojure test stacktrace]
        [aleph core]
        jiraph))

(defgraph G
          :path "db" :create true
          (layer :graph))

(defn test-graph []
  (with-graph G 
    (clear-graph)
    (node "ROOT" :label :self)
    (with-nodes! [music :music
                  synths :synths
                  kick :kick
                  bass :bass]
                 (edge "ROOT" music :label :music)
                 (edge music synths :label :synths)
                 (edge synths kick :label :synth)
                 (edge synths bass :label :synth))))

; Currently implements a query tree equivalent to:
; (path [synth [:music :synths :synth]]
;   (where (= :kick (:label synth))))
(defn traverse-test []
  (let [p1 (param-op)
        t1 (traverse-op (:uuid p1) :music)
        j1 (join-op p1 t1)
        t2 (traverse-op (:uuid t1) :synths)
        j2 (join-op j1 t2)
        t3 (traverse-op (:uuid t2) :synth)
        j3 (join-op j2 t3)
        sel-pred {:type :plasma.operator/predicate
                  :property :label
                  :operator '=
                  :value :kick}
        sel (select-op j3 (:uuid t3) sel-pred)
        proj (project-op sel (:uuid t3))]
    {:p1 p1
     :t1 t1
     :j1 j1
     :t2 t2
     :j2 j2
     :t3 t3
     :j3 j3
     :sel sel
     :proj proj}))

(defn result [q]
  (channel-seq (get-in q [:proj :out])))
