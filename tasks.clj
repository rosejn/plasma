(defn start-ng-server [host]
  (bake (:import vimclojure.nailgun.NGServer)
        [host host]
        (vimclojure.nailgun.NGServer/main (into-array [host]))))

(deftask ng #{}
  "Start the nailgun server."
  (start-ng-server "127.0.0.1"))

