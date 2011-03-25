(ns plasma.util)

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
