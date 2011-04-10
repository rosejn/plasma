(ns test-utils
  (:use [plasma core operator connection]
        [clojure test stacktrace]
        [jiraph graph])
  (:require [lamina.core :as lamina]))

(def G (graph "test/db"))

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
                 (make-edge root-id (make-node :label :net) :label :net)
                 (make-edge root-id music :label :music)
                 (make-edge music synths :label :synths)
                 (make-edge synths bass :label :synth)
                 (make-edge synths hat  :label :synth)
                 (make-edge synths kick :label :synth)
                 (make-edge synths snare :label :synth)
                 (make-edge root-id sessions :label :sessions)
                 (make-edge sessions take-six :label :session)
                 (make-edge take-six kick :label :synth)
                 (make-edge take-six bass :label :synth)
                 (make-edge sessions red-pill :label :session)
                 (make-edge red-pill hat   :label :synth)
                 (make-edge red-pill snare :label :synth)
                 (make-edge red-pill kick  :label :synth))))

(defn test-fixture [f]
  (with-graph G
    (clear-graph)
    (test-graph)
    (f)))

(defn close-peers
  [peers]
  (doseq [p peers]
    (close p)))

