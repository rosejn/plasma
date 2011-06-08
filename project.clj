(defproject plasma "0.2.0-SNAPSHOT"
  :description "Distributed graph computing."
;  :repositories {"clojure-releases" "http://build.clojure.org/releases"}
  :dependencies [[org.clojure/clojure "1.2.0"]
;                 [org.clojure/clojure "1.3.0-alpha8"]
                 [org.clojure/clojure-contrib "1.2.0"]
                 [aleph "0.2.0-alpha1"]
                 [jiraph "0.6.1-SNAPSHOT"]
                 [clojure-protobuf "0.3.4"]
                 [org.clojars.overtone/vijual "0.2.1"]
                 [logjam "0.1.0-SNAPSHOT"]
                 [ring/ring-core "0.3.1"]
                 [org.bitlet/weupnp "0.1.2-SNAPSHOT"]]
  :dev-dependencies [[marginalia "0.5.1-SNAPSHOT"]]
  :tasks [marginalia.tasks])

(import '[java.io File])
(use '[clojure.java.io :only (delete-file)])

(defn ls-files
  [dir & [suffix]]
  (let [in-dir (.list (File. dir))]
    (if suffix
      (let [re (re-pattern (str ".*." suffix))]
        (filter #(re-matches  re %)
                in-dir))
      in-dir)))

(deftask clean
  "Remove log files and test databases."
  (try
    (doseq [f (ls-files "./" "log")]
      (println f)
      (delete-file f))
    (doseq [f (ls-files "db")]
      (println delete-file f)
      (delete-file f))
    (catch Exception e
      (println "Error running Plasma clean task...")
      (.printStackTrace e))))
