(ns plasma.route-test
  (:use [plasma config util core connection peer bootstrap route]
        test-utils
        clojure.test
        clojure.stacktrace)
  (:require [logjam.core :as log]
            [lamina.core :as lamina]
            [plasma.query :as q]))

(defn bootstrap-peers
  [peers strap-url]
  (doall
    (for [p peers]
      (bootstrap p strap-url))))

(defn bootstrapped-peers
  [n-peers]
  (let [port (+ 5000 (rand-int 5000))
        strapper (bootstrap-peer "db/strapper" {:port port})
        strap-url (plasma-url "localhost" port)
        peers (make-peers n-peers (inc port)
                (fn [i]
                  (clear-graph)
                    (make-edge ROOT-ID (make-node) :net)))]
    (bootstrap-peers peers strap-url)
    [strapper peers]))



