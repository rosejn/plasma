(ns plasma.upnp
  (import [org.bitlet.weupnp GatewayDiscover PortMappingEntry]))

(defn gateway []
  (let [discoverer (GatewayDiscover.)]
    (.discover discoverer)
    (.getValidGateway discoverer)))

(defn addr-info
  []
  (let [g (gateway)]
    {:local-addr (.getLocalAddress g)
     :public-addr (.getExternalIPAddress g)}))

(defn set-port-forward
  ([port service]
   (set-port-forward port "TCP" service))
  ([port proto service]
   (let [entry (PortMappingEntry.)
         g (gateway)
         {:keys [local-addr public-addr]} (addr-info)
         addr (.getHostAddress local-addr)]
     (if-not (.getSpecificPortMappingEntry g port proto entry)
       (.addPortMapping g port port addr proto service)))))

(defn clear-port-forward
  ([port]
   (clear-port-forward port :tcp))
  ([port proto]
   (let [g (gateway)
         proto (cond
                 (string? proto) (.toUpperCase proto)
                 (keyword? proto) (.toUpperCase (name proto)))]
     (.deletePortMapping g port proto))))

