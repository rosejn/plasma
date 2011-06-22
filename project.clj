(defproject plasma "0.3.0-SNAPSHOT"
  :description "A graph database query engine."
  :dependencies [[org.clojure/clojure "1.2.0"]
                 [org.clojure/clojure-contrib "1.2.0"]
                 [jiraph "0.7.0-SNAPSHOT"]
                 [tokyocabinet "1.24.1-SNAPSHOT"]
                 [lamina "0.4.0-alpha3-SNAPSHOT"]
                 [clojure-protobuf "0.3.4"]
                 [org.clojars.overtone/vijual "0.2.1"]
                 [logjam "0.1.0-SNAPSHOT"]]
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
