(ns plasma.util
  (:import (java.util.concurrent Executors TimeUnit)))

(defn plasma-url
  [host port]
  (str "plasma://" host ":" port))

(defn url-map [url]
  (let [match (re-find #"(.*)://([a-zA-Z-_.]*):([0-9]*)" url)
        [_ proto host port] match]
    {:proto proto
     :host host
     :port (Integer. port)}))

(defn uuid
  "Creates a random, immutable UUID object that is comparable using the '=' function."
  [] (str "UUID:" (. java.util.UUID randomUUID)))

(defn uuid? [s]
  (and (string? s)
       (= (seq "UUID:") (take 5 (seq s)))))

(defn current-time []
  (System/currentTimeMillis))

(defmacro unless
  [expr form]
  (list 'if expr nil form))

(defn regexp?
  [obj]
  (= java.util.regex.Pattern (type obj)))

(defn map-fn
  [m key fn & args]
  (assoc m key (apply fn (get m key) args)))

(defn periodically
  "Executes a function every period milliseconds.  Returns a function that can
  be called to terminate the execution.  If true is passed as the argument to 
  this function it will terminate immediately rather than waiting for the
  already scheduled tasks to complete."
  [period fun]
  (let [s (Executors/newSingleThreadScheduledExecutor)]
    (.scheduleAtFixedRate s fun (long 0) (long period) TimeUnit/MILLISECONDS)
    (fn [& [now?]]
      (if now?
        (.shutdownNow s)
        (.shutdown s)))))

