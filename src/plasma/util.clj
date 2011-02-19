(ns plasma.util)

(defmacro unless 
  [expr form]
  (list 'if expr nil form))

