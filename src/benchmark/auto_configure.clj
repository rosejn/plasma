(ns benchmark.auto-configure
  (:use
    [plasma config]
    [plasma.net peer presence]))

(config :presence true)

(def peer (peer))

(Thread/sleep 12000)

(let [ps (get-peers peer)]
  (println "Got" (count ps) "peers\n\n" ps))

(System/exit)
