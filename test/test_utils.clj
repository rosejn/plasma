(ns test-utils
  (:use [plasma graph config util api]
        [plasma.net peer url connection bootstrap]
        [plasma.query operator]
        [clojure test stacktrace])
  (:require [lamina.core :as lamina]
            [jiraph.graph :as jiraph]))

(def G (open-graph "db/test"))

(defmacro let-nodes [bindings & body]
  (let [nodes (map (fn [[node-sym props]]
                     (let [props (if (keyword? props)
                                   {:label props}
                                   props)]
                       [node-sym `(make-node ~props)]))
                   (partition 2 bindings))
        nodes (vec (apply concat nodes))]
    `(let ~nodes ~@body)))

(defmacro edges! [edge-specs]
  (cons 'do
        (mapcat (fn [[src tgt edge]]
                  (list `(make-edge ~src ~tgt ~edge)))
                (partition 3 edge-specs))))

(defn test-graph []
  (let [root-id (root-node-id)]
    (let-nodes [net      :net
                music    :music
                synths   :synths
                kick    {:label :kick  :score 0.8}
                hat     {:label :hat   :score 0.3}
                snare   {:label :snare :score 0.4}
                bass    {:label :bass  :score 0.6}
                sessions :sessions
                take-six :take-six
                red-pill :red-pill]
               (edges!
                 [root-id  net      :net
                  root-id  music    :music
                  music    synths   :synths
                  synths   bass     :synth
                  synths   hat      :synth
                  synths   kick     :synth
                  synths   snare    :synth
                  root-id  sessions :sessions
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

(defn make-peers
  "Create n peers, each with a monotically increasing port number.
  Then run (fun i) with the peer graph bound to initialize each peer,
  and i being the index of the peer being created."
  [n start-port fun]
  (doall
    (for [i (range n)]
        (let [p (peer {:port (+ start-port i)
                       ;:path (str "db/peer-" i)
                       }
                      )]
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
        strapper (bootstrap-peer {:path "db/strapper" :port port})
        strap-url (plasma-url "localhost" port)
        peers (make-peers n (inc port)
                (fn [i]
                  (clear-graph)
                    (make-edge ROOT-ID (make-node) :net)))]
    (bootstrap-peers peers strap-url)
    [strapper peers]))
