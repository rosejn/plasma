(ns plasma.heartbeat-test
  (:use [plasma graph util connection peer bootstrap]
        test-utils
        clojure.test
        clojure.stacktrace)
  (:require [logjam.core :as log]
            [lamina.core :as lamina]
            [plasma.query :as q]))

;(log/file [:peer :heart :con] "peer.log")

(deftest test-failure-detection
  )
