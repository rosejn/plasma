(ns plasma.query-test
  (:use [plasma.core] :reload-all)
  (:use clojure.test
        clojure.stacktrace
        jiraph))

(defgraph G
          :path "db" :create true
          (layer :graph))

;(set-graph! TEST-GRAPH)

(defn test-graph []
  (with-graph G
    (node ROOT-ID :label :self)
    (with-nodes! [net  :net
                  social :social
                  music :music
                  fx :fx
                  sessions :sessions
                  synths :synths
                  distortion :distortion
                  chorus :chorus
                  compressor :compressor
                  take-six :take-six
                  red-pill :red-pill
                  z-lead   {:label :z-lead :score 9}
                  big-kick {:label :big-kick :score 7}
                  moon-pad {:label :moon-pad :score 2}
                  fat-bass {:label :fat-bass :score 4}]
      (edge ROOT-ID net    :label :net)
      (edge ROOT-ID social :label :social)
      (edge ROOT-ID music  :label :music)
      (edge music fx :label :fx)
      (edge music synths :label :synths)
      (edge music sessions :label :sessions)
      (edge fx distortion :label :effect)
      (edge fx chorus :label :effect)
      (edge fx compressor :label :effect)
      (edge synths z-lead :label :synth)
      (edge synths big-kick :label :synth)
      (edge synths moon-pad :label :synth)
      (edge synths fat-bass :label :synth))))

(use-fixtures :each (fn [f]
                      (with-graph G
                        (clear-graph)
                        (test-graph)
                        (f))))

(def SYNTHS #{:z-lead :big-kick :moon-pad :fat-bass :foo})

(deftest path-queries
  (let [a (query [:music :synths :synth])
        b (query [s [:music :synths :synth]] s)
        c (query [synths [:music :synths]
                  insts  [synths :synth]]
                 insts)
        d (query [m [:music]
                  s [m :synths]
                  i [s :synth]] i)
        results (map #(set (run-query %)) [a b c d])]
    (is (every? SYNTHS (map #(:label (find-node %)) (run-query a))))
    (is (every? #(= (first results) %) results))))

(deftest where-predicates
  (let [a (query [s [:music :synths :synth]]
                 (where (= :z-lead (:label s)))
                 s)
        res-a (run-query a)
        b (query [s [:music :synths :synth]]
                 (where (> (:score s) 5))
                 s)
        res-b (run-query b)]
    (is (= 1 (count res-a)))
    (is (= :z-lead (:label (find-node (first res-a)))))
    (is (= 2 (count res-b)))
    ))

(defn test-plasma []
  (binding [*test-out* *out*]
    (run-all-tests #"plasma")))

;(query [s-root [:synths]
;        inst  [s-root :synth]]
;  (where
;    (= :bass (:label inst)))
;  inst)
;
; Path expression = a sequence of edge :label properties
; * expression result sets can be bound to query vars
; * the *root* var is bound to the root of the execution graph
;(query [s [*root* :music :sessions :session]] s)
;(query [:music :sessions :session])

; where can be used to filter nodes by applying predicates
; to the bound path expression results
; * predicates can treat the bound nodes as key/value maps
; so to lookup a property value just (:key var)
;(query [session [(root) :music :sessions :session]
;                member [session :member]
;                synth   [session :synth]]
;         (where
;           (= (:name member) "Jon")
;           (= (:name synth) "z-lead"))
;         session)

; Query returns a query object that can be nested arbitrarily
; inside other queries.  Set operations and additional filters
; can be used to process query results.
;(query
;    (intersection
;         (query [s [:music :sessions :session]
;                            inst [s :synth]]
;                    (where (= (:name s) "take-six"))
;                      inst)
;         (query [s [:music :sessions :session]
;                            inst [s :synth]]
;                    (where (= (:name s) "red-pill"))
;                      inst)))

; Iterate and recurse can be used to repeatedly execute a query,
; possibly re-using the results of one execution as input to
; the successive execution.
;(query [p (iterate 10 *root*
;                   (query (choose 1
;                                  (query [?start? :net :peer]))))
;        synths [p :music :synths :synth]]
;       synths)
;
;(query [p (recurse (= (:id ?node?) 500) *root*
;                   (take 1 (sort :id
;                                 (query [p [?start?:net :peer]]
;                                        (where (<= (:id p) 500))))))]
;       p)

