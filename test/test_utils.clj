(ns test-utils
  (:use [plasma graph util api operator]
        [clojure test stacktrace])
  (:require [lamina.core :as lamina]
            [plasma.construct :as c]
            [jiraph.graph :as jiraph]))

(def G (open-graph "db/test"))

(defn test-graph []
  (c/construct*
    (-> (c/nodes [root     ROOT-ID
                net      :net
                music    :music
                synths   :synths
                kick    {:label :kick  :score 0.8}
                hat     {:label :hat   :score 0.3}
                snare   {:label :snare :score 0.4}
                bass    {:label :bass  :score 0.6}
                sessions :sessions
                take-six :take-six
                red-pill :red-pill])
      (c/edges
        [root     net      :net
         root     music    :music
         music    synths   :synths
         synths   bass     {:label :synth :favorite true}
         synths   hat      :synth
         synths   kick     :synth
         synths   snare    :synth
         root     sessions :sessions
         sessions take-six :session
         take-six kick     :synth
         take-six bass     :synth
         sessions red-pill :session
         red-pill hat      :synth
         red-pill snare    :synth
         red-pill kick     :synth]))))

(defn graph-apply [g f]
  (jiraph/with-graph g
    (f)))

(defn test-fixture [f]
  (graph-apply G #(do
                    (clear-graph)
                    (test-graph)
                    (f))))

(defn query-tree-results
  [tree & [timeout]]
  (let [chan (get-in (:ops tree) [(:root tree) :out])
        timeout (or timeout 1000)]
    (lamina/lazy-channel-seq chan timeout)))
