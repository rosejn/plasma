(ns plasma.core
  (:use plasma.ns)
  (:require [plasma util config api graph viz web]
            [plasma.net peer url connection route bootstrap] ; heartbeat]
            [plasma.query core construct]
            [logjam.core :as log]))

(immigrate
  'plasma.util
  'plasma.config
  'plasma.api
  'plasma.graph
  'plasma.query.construct
  'plasma.viz
  'plasma.net.url
  'plasma.net.peer
  'plasma.net.route)
;  'plasma.net.bootstrap
;  'plasma.net.heartbeat
;  'plasma.query.core)
