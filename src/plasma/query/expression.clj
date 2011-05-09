(ns plasma.query.expression
  (:use [plasma util]
        [plasma.query helpers]
        [clojure.walk :only (postwalk)]
        clojure.contrib.generic.math-functions
        clojure.stacktrace)
  (:require [logjam.core :as log]))

(def *pvars* nil)

(defmacro def-unary-op [op-sym]
  (let [resd (str (.getName (.ns (resolve op-sym))))
        fn-sym (symbol resd (str op-sym))]
    `(defn ~(symbol (str op-sym  "*"))
       [~'v]
       (list '~fn-sym ~'v))))

(def-unary-op inc)
(def-unary-op dec)
(def-unary-op nil?)
(def-unary-op zero?)
(def-unary-op pos?)
(def-unary-op neg?)
(def-unary-op even?)
(def-unary-op odd?)

(def-unary-op abs)
(def-unary-op acos)
(def-unary-op asin)
(def-unary-op atan)
(def-unary-op atan2)
(def-unary-op ceil)
(def-unary-op conjugate)
(def-unary-op cos)
(def-unary-op exp)
(def-unary-op floor)
(def-unary-op log)
(def-unary-op rint)
(def-unary-op round)
(def-unary-op sgn)
(def-unary-op sin)
(def-unary-op sqr)
(def-unary-op sqrt)
(def-unary-op tan)

(defmacro def-binary-op [op-sym]
  (let [resd (str (.getName (.ns (resolve op-sym))))
        fn-sym (symbol resd (str op-sym))]
    `(defn ~(symbol (str op-sym  "*"))
       [~'a ~'b]
       (list '~fn-sym ~'a ~'b))))

(defn mul*
  [a b]
  (list '* a b))

(defn div*
  [a b]
  (list '/ a b))


(def-binary-op =)
(def-binary-op ==)
(def-binary-op not=)
(def-binary-op <)
(def-binary-op >)
(def-binary-op >=)
(def-binary-op <=)
(def-binary-op +)
(def-binary-op -)
(def-binary-op mod)
(def-binary-op bit-and)
(def-binary-op bit-or)
(def-binary-op bit-xor)
(def-binary-op bit-flip)
(def-binary-op bit-not)
(def-binary-op bit-clear)
(def-binary-op bit-set)
(def-binary-op bit-shift-left)
(def-binary-op bit-shift-right)
(def-binary-op bit-test)
(def-binary-op pow)

(defmacro def-trinary-op [op-sym]
  (let [resd (str (.getName (.ns (resolve op-sym))))
        fn-sym (symbol resd (str op-sym))]
    `(defn ~(symbol (str op-sym  "*"))
       [~'a ~'b ~'c]
       (list '~fn-sym ~'a ~'b ~'c))))

(def-trinary-op approx=)

(def EXPRESSION-SYMBOLS
  '{
    inc             plasma.query.expression/inc*
    dec             plasma.query.expression/dec*
    nil?            plasma.query.expression/nil?*
    zero?           plasma.query.expression/zero?*
    pos?            plasma.query.expression/pos?*
    neg?            plasma.query.expression/neg?*
    even?           plasma.query.expression/even?*
    odd?            plasma.query.expression/odd?*

    abs             plasma.query.expression/abs*
    acos            plasma.query.expression/acos*
    asin            plasma.query.expression/asin*
    atan            plasma.query.expression/atan*
    atan2           plasma.query.expression/*
    ceil            plasma.query.expression/ceil*
    conjugate       plasma.query.expression/conjugate*
    cos             plasma.query.expression/cos*
    exp             plasma.query.expression/exp*
    floor           plasma.query.expression/floor*
    log             plasma.query.expression/log*
    rint            plasma.query.expression/rint*
    round           plasma.query.expression/round*
    sgn             plasma.query.expression/sgn*
    sin             plasma.query.expression/sin*
    sqr             plasma.query.expression/sqr*
    sqrt            plasma.query.expression/sqrt*
    tan             plasma.query.expression/tan*

    =               plasma.query.expression/=*
    ==              plasma.query.expression/==*
    not=            plasma.query.expression/not=*
    <               plasma.query.expression/<*
    >               plasma.query.expression/>*
    >=              plasma.query.expression/>=*
    <=              plasma.query.expression/<=*
    +               plasma.query.expression/+*
    -               plasma.query.expression/-*
    mod             plasma.query.expression/mod*
    /               plasma.query.expression/div*
    *               plasma.query.expression/mul*

    bit-and         plasma.query.expression/bit-and*
    bit-or          plasma.query.expression/bit-or*
    bit-xor         plasma.query.expression/bit-xor*
    bit-flip        plasma.query.expression/bit-flip*
    bit-not         plasma.query.expression/bit-not*
    bit-clear       plasma.query.expression/bit-clear*
    bit-set         plasma.query.expression/bit-set*
    bit-shift-left  plasma.query.expression/bit-shift-left*
    bit-shift-right plasma.query.expression/bit-shift-right*
    bit-test        plasma.query.expression/bit-test*
    })

(defn- add-property-op
  [{root :root :as plan} {:keys [id pvar property]}]
  (let [bind-op (get (:pbind plan) (symbol pvar))
        prop-op {:type :property
                 :id id
                 :deps [root]
                 :args [bind-op [property]]}]
    (append-root-op plan prop-op)))

(defn- add-expression-op
  [plan {:keys [op a b id]}]
  (let [deps (filter
               #(or (= :pvar-property (:type %))
                    (= :expression (:type %)))
               [a b])
        plan (reduce
               (fn [plan dep]
                 (case (:type dep)
                   :pvar-property (add-property-op plan dep)
                   :expression (add-expression-op plan dep)))
               plan
               deps)
        a (or (:id a) a)
        b (or (:id b) b)]
    (append-root-op plan {:type :expression
                          :id id
                          :deps [(:root plan)]
                          :args [op [a b]]})))

(defn pvar-prop
  [pvar prop]
  (let [pname (gensym (str pvar "-" (name prop) "-"))
        pvp {:type :pvar-property
             :name pname
             :pvar (str pvar)
             :property prop}]
    (swap! *pvars* conj pvp)
    pname))

(defn pvar-property
  "Converts a property lookup on a path variable to a map."
  [form]
  (let [pvar (second (first (filter seq? form)))
        prop (first (filter keyword? form))]
    `(pvar-prop '~pvar ~prop)))

(defn- pvar-getter?
  "Is it a property lookup on a path variable?  Recognizes map
  lookup forms such as (:foo m) or (m :foo)."
  [x]
  (and (list? x)
       (= (count x) 2)
       (or (keyword? (first x))
           (keyword? (second x)))
       (or (and (seq? (first x))
                (= 'quote (ffirst x)))
           (and (seq? (second x))
                (= 'quote (first (second x)))))))

(defn rebind-expr-ops
  [form]
  (postwalk
    (fn [x]
      (cond
        (pvar-getter? x) (pvar-property x)
        (contains? EXPRESSION-SYMBOLS x) (EXPRESSION-SYMBOLS x)
        :else x))
    form))

(defn eval-with-vars [vars expr]
  (let [bindings (flatten (seq vars))
        res (eval (list 'let (vec bindings) expr))]
    (log/format :flow "[eval-with-vars]\n(let %s\n  %s)\n=> %s"
                (vec bindings) expr res)
    res))

#_(let [foo #"buck*" bit 0]
  (query p1 (->
              (q/path [b [:net foo]])
              (q/expr [c (* 200 (:bit b))])
              (where (and
                       (= (:bit b) c)
                       (= (:id b) "UUID:0e214a7f-c4f8-42d8-a6df-9f649ea4aefc"))
              (q/project [b :bit :id])))))

