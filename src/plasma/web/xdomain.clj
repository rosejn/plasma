(ns plasma.web.xdomain
   (:use [aleph tcp]))

(def SPF "<cross-domain-policy><allow-access-from domain='*' to-ports='*'/></cross-domain-policy>\n\n\0")

(defn spf-handler [channel connection-info]
  (receive-all channel (fn [req]
			 (if (= "<policy-file-request/>\0"
				(byte-buffer->string req))
			   (let [f (string->byte-buffer SPF)]
			     (enqueue-and-close channel f))
			   (println (byte-buffer->string req))))))

(defn start-policy-server []
  (start-tcp-server spf-handler 
                    {:port 843}))

