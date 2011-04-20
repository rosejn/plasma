(ns plasma.config
  (:use plasma.util))

(defonce config*
  (ref {:plasma-version      "0.1.0"            ; version number, for protocol versioning
        :protocol            "plasma"           ; default peer protocol
        :peer-port            4242              ; listening for incoming socket connections
        :presence-port        4243              ; UDP broadcast port for presence messaging
        :presence-ip         "224.242.242.242"  ; IPs 224.0.0.0 - 239.255.255.255 are reserved for multicast
        :presence-period      4                 ; presence broadcast every N seconds
        :connection-cache-limit 50                ; max open connections
        :peer-id              (uuid)            ; TODO: store this somewhere?
        :meta-id              "UUID:META"       ; special UUID for graph meta-data node

;        :db-path              "db"
        }))

(defn config
  "Lookup a config value."
  ([] @config*)
  ([key] (get @config* key)))

