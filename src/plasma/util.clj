(ns plasma.util)

(defmacro unless
  [expr form]
  (list 'if expr nil form))

(defn regexp?
  [obj]
  (= java.util.regex.Pattern (type obj)))
