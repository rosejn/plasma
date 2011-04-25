(ns test-utils
  (:use [plasma core config util url operator connection peer bootstrap]
        [clojure test stacktrace])
  (:require [lamina.core :as lamina]
            [jiraph.graph :as jiraph]))

(def G (open-graph "db/test"))

(defn test-graph []
  (let [root-id (root-node-id)]
    (with-nodes! [net      :net
                  music    :music
                  synths   :synths
                  kick    {:label :kick  :score 0.8}
                  hat     {:label :hat   :score 0.3}
                  snare   {:label :snare :score 0.4}
                  bass    {:label :bass  :score 0.6}
                  sessions :sessions
                  take-six :take-six
                  red-pill :red-pill]
                 (make-edge root-id net       :net)
                 (make-edge root-id music     :music)
                 (make-edge music synths      :synths)
                 (make-edge synths bass       :synth)
                 (make-edge synths hat        :synth)
                 (make-edge synths kick       :synth)
                 (make-edge synths snare      :synth)
                 (make-edge root-id sessions  :sessions)
                 (make-edge sessions take-six :session)
                 (make-edge take-six kick     :synth)
                 (make-edge take-six bass     :synth)
                 (make-edge sessions red-pill :session)
                 (make-edge red-pill hat      :synth)
                 (make-edge red-pill snare    :synth)
                 (make-edge red-pill kick     :synth))))

(defn test-fixture [f]
  (jiraph/with-graph G
    (clear-graph)
    (test-graph)
    (f)))

(defn make-peers
  "Create n peers, each with a monotically increasing port number.
  Then run (fun i) with the peer graph bound to initialize each peer,
  and i being the index of the peer being created."
  [n start-port fun]
  (doall
    (for [i (range n)]
        (let [p (peer (str "db/peer-" i)
                      {:port (+ start-port i)})]
          (with-peer-graph p
                      (fun i)
                      p)))))

(defn close-peers
  [peers]
  (doseq [p peers]
    (close p)))

(defn bootstrap-peers
  [peers strap-url]
  (doall
    (for [p peers]
      (bootstrap p strap-url))))

(defn bootstrapped-peers
  [n]
  (let [port (+ 5000 (rand-int 5000))
        strapper (bootstrap-peer "db/strapper" {:port port})
        strap-url (plasma-url "localhost" port)
        peers (make-peers n (inc port)
                (fn [i]
                  (clear-graph)
                    (make-edge ROOT-ID (make-node) :net)))]
    (bootstrap-peers peers strap-url)
    [strapper peers]))
