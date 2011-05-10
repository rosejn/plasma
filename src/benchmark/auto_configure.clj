(ns benchmark.auto-configure
  (:use
    [plasma config]
    [plasma.net peer presence]))

(config :presence true)

(def p (peer))

(Thread/sleep 12000)

(let [ps (get-peers p)]
  (println "Got" (count ps) "peers\n\n" ps))

(System/exit)
