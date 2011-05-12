(ns example.query
  (:use plasma.core
        test-utils)
  (require [plasma.query.core :as q]
           [logjam.core :as log]))

(def acme-graph (open-graph "db/example.query"))

(defn make-acme-graph
  []
  (clear-graph)
  (let [root (root-node-id)] ; acme is root
    (let-nodes [
                alice         {:label :alice :gender :female}
                bob           {:label :bob   :gender :male}
                maria         {:label :maria :gender :female}
                lugano        :lugano-office
                geneva        :geneva-office
                zurich        :zurich-office
                product-alpha {:label :alpha :price 110.00}
                product-beta  {:label :beta  :price 600.00}
                comp-1        {:label :component-1 :cost 24.00}
                comp-2        {:label :component-2 :cost 8.00}
                comp-3        {:label :component-3 :cost 75.00}
                comp-4        {:label :component-4 :cost 120.00}
                ]
      (edges!
        [
         root alice         :person
         root bob           :person
         root maria         :person
         root lugano        :location
         root geneva        :location
         root zurich        :location
         alice geneva       :manages
         geneva alice       :managed-by
         bob zurich         :manages
         zurich bob         :managed-by
         maria lugano       :manages
         lugano maria       :managed-by
         root product-alpha :product
         root product-beta  :product
         product-alpha lugano :made-in
         product-beta  lugano :made-in
         product-alpha comp-1 :component
         product-alpha comp-2 :component
         product-beta  comp-2 :component
         product-beta  comp-3 :component
         product-beta  comp-4 :component
         comp-1 zurich :made-in
         comp-2 zurich :made-in
         comp-3 geneva :made-in
         comp-4 geneva :made-in
         ]))))

(defn setup-acme
  []
  (graph-apply acme-graph make-acme-graph))

(defn acme
  "Define a helper function to query our acme graph."
  [q]
  (graph-apply acme-graph #(query q)))

; basic path selection
;  - no binding
;  - with binding
;  - multiple bindings

(defn products-a
  "Select all of the products connected to the root node with an edge
  labeled :product."
  []
  (q/path [:product]))

(defn products
  "Select the products exactly as before, but create a path binding variable
  that can be referred to later in the query."
  []
  (q/path [product [:product]]))

(defn managers
  "Select the manager of each location, and create a query binding
  to these manager nodes so they can be referred to within the query."
  []
  (q/path [manager [:location :managed-by]]))

(defn components
  "Select the components that make up ACME products.  This example shows how
  you can use multi-segment bindings in a path expression."
  []
  (q/path [product [:product]
           component [product :component]]))

; projection
(defn product-prices
  "By default queries only return the node IDs of the result set, so
  if you want to get specific properties of a node you need to project
  those properties.  Note how the query binding variable \"product\" is
  quoted, which differentiates it from a normal program variable.  This
  allows you to mix both run-time program variables and query execution
  time query bindings."
  []
  (-> (products)
    (q/project ['product :label :price])))

(defn with-product-info
  "It can be useful to have a projection function like this, which you
  can use to wrap queries that you want to project some specific properties."
  [query]
  (-> query
    (q/project ['product :label :price])))

;For example:
;  (acme (with-product-info (products)))

(defn with-component-info
  "It can be useful to have a projection function like this, which you
  can use to wrap queries that you want to project some specific properties."
  [query]
  (-> query
    (q/project ['component :label :cost])))

;For example:
;  (acme (with-component-info (components)))

; Could we do something like this?
(comment def product-component-prices
  (-> components
    (q/expr [total-cost (sum (:price 'component))])
    (q/project ['product :label] 'total-cost)))

; where filtering
; - simple property equality
; - with mathematical expression
; - matching interesting graph pattern using multiple bindings

(defn managers-by-gender
  "The where form can be used to filter the results based on property
  values of bound nodes."
  [gender]
  (-> (managers)
    (q/where (= gender (:gender 'manager)))))

(defn female-managers
  []
  (managers-by-gender :female))

(defn male-managers
  []
  (managers-by-gender :male))

(defn profitable-components
  "Where forms can be constructed using both arithmetic and boolean
  expressions.  Here is a function that returns a query which will
  return all components that have a marked-up cost greater than
  a given threshold."
  [markup threshold]
  (-> (components)
    (q/where (> (* markup (:cost 'component)) threshold))))

; For example:
;  (acme (profitable-components 3 100))

; or with the label and price
;  (acme (with-component-info (profitable-components 3 100)))

(defn with-components-from
  "The where expression can pertain to multiple binding points in the query
  as well.  For example, this function will return all products that
  have components which were made in the specified location."
  [loc]
  (-> (q/path [location [(components) :made-in]])
    (q/where (= loc (:label 'location)))))

; order-by
; - asc
; - desc
(def components-by-cost
  "By default the sort order is ascending."
  (-> components
    (q/order-by 'component :cost)))

(def products-by-price
  "But you can specify descending like so."
  (-> products
    (q/order-by 'product :price :desc)))

; limit
; - need offset?
(def cheapest-component
  (-> components-by-cost
    (q/limit 1)))

; choose
(defn n-random-components
  [n]
  (-> components
    (q/choose n)))

; group-by
(defn components-by-location
  [loc]
  (-> (with-components-from loc)
    (order-by 'location :label)
    (with-component-info)))

; count
(def num-products
  (-> products
    (count*)))

(defn num-products-with-components-from
  [loc]
  (count* (with-components-from loc)))

; average
(def average-product-price
  (-> products
    (avg 'product :price)))

; min/max
(def cheapest-component
  (-> components
    (min 'component :price)))

(def most-expensive-component
  (-> components
    (min 'component :price)))

; distinct?
