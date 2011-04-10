(ns plasma.heartbeat-test
  (:use [plasma core util connection peer bootstrap]
        jiraph.graph
        test-utils
        clojure.test
        clojure.stacktrace)
  (:require [logjam.core :as log]
            [lamina.core :as lamina]
            [plasma.query :as q]))

;(log/file [:peer :heart :con] "peer.log")

(deftest test-failure-detection
  )
