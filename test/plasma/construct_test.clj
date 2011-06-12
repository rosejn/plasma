(ns plasma.construct-test
  (:use [plasma util graph viz construct]
        [clojure test stacktrace]
        test-utils)
  (:require [logjam.core :as log]
            [plasma.query :as q]))

(deftest construct-test
  (clear-graph)
  (make-edge ROOT-ID (make-node {:label :locations}) :locations)
  (let [spec (-> (nodes [factory   {:name "Jonestown" :employee-count 65}
                         manager   {:label :manager :name "Larry Jones"}
                         widget     :widget-a
                         root       ROOT-ID
                         locations (q/path [:locations])])
                (edges [locations factory :factory
                        factory widget :manufactures
                        factory manager :manager
                        root manager :manager]))]
    (construct* spec)
    (let [m1 (-> (q/path [f [:locations :factory]
                            m [f :manager]])
               (q/where (= "Jonestown" (:name 'f)))
               (q/project ['m :name]))
            m2 (-> (q/path [m [:manager]])
                 (q/where (= "Larry Jones" (:name 'm)))
                 (q/project ['m :name]))]
      (= (q/query m1) (q/query m2)))))

(use-fixtures :each test-fixture)
