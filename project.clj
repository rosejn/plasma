(defproject plasma "0.1.0-SNAPSHOT"
  :description "Distributed graph computing."
;  :repositories [["jboss" "http://repository.jboss.org/nexus/content/groups/public/"]]
  :dependencies [[org.clojure/clojure "1.2.0"]
                 [org.clojure/clojure-contrib "1.2.0"]
;                 [org.jboss.netty/netty "3.2.1.Final"]
;                 [aleph-core "0.6.0-SNAPSHOT"]
                 [aleph "0.1.2-SNAPSHOT"]
                 [jiraph "0.1.3-SNAPSHOT"]
                 [clj-serializer "0.1.0"]
                 [vijual "0.1.0-SNAPSHOT"]]
  :dev-dependencies [[native-deps "1.0.0"]])
