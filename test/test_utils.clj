(ns test-utils
  (:use [plasma core operator]
        [clojure test stacktrace]
        [lamina core]
        [jiraph graph]))

(def G
  {:graph (layer "test/db")})

(defn test-graph []
  (let [root-id (root-node)]
    (with-nodes! [music :music
                  synths :synths
                  kick  {:label :kick  :score 0.8}
                  hat   {:label :hat   :score 0.3}
                  snare {:label :snare :score 0.4}
                  bass  {:label :bass  :score 0.6}
                  sessions :sessions
                  take-six :take-six
                  red-pill :red-pill]
                 (edge root-id (node :label :net) :label :net)
                 (edge root-id music :label :music)
                 (edge music synths :label :synths)
                 (edge synths bass :label :synth)
                 (edge synths hat  :label :synth)
                 (edge synths kick :label :synth)
                 (edge synths snare :label :synth)
                 (edge root-id sessions :label :sessions)
                 (edge sessions take-six :label :session)
                 (edge take-six kick :label :synth)
                 (edge take-six bass :label :synth)
                 (edge sessions red-pill :label :session)
                 (edge red-pill hat   :label :synth)
                 (edge red-pill snare :label :synth)
                 (edge red-pill kick  :label :synth))))

(defn test-fixture [f]
  (with-graph G
    (clear-graph)
    (test-graph)
    (f)))
