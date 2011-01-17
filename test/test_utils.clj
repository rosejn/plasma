(ns test-utils
  (:use [plasma core operator]
        [clojure test stacktrace]
        [lamina core]
        [jiraph graph]))

(def G
  {:graph (layer "test/db")})

(defn test-graph []
  (node ROOT-ID :label :self)
  (with-nodes! [music :music
                synths :synths
                kick  {:label :kick  :score 0.8}
                hat   {:label :hat   :score 0.3}
                snare {:label :snare :score 0.4}
                bass  {:label :bass  :score 0.6}]
               (edge ROOT-ID (node :label :net) :label :net)
               (edge ROOT-ID music :label :music)
               (edge music synths :label :synths)
               (edge synths bass :label :synth)
               (edge synths hat  :label :synth)
               (edge synths kick :label :synth)
               (edge synths snare :label :synth)))

(defn test-fixture [f]
  (with-graph G
    (clear-graph)
    (test-graph)
    (f)))
