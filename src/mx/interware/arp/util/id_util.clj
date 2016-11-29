(ns mx.interware.arp.util.id-util
  (:import (java.util Random UUID))
  )

(defn uuid [] (str (UUID/randomUUID)))

(defn key-juxt [& ks]
  (fn [e] (vec (flatten (map (fn [k] [(str k) (k e)]) ks)))))

(defn random-hex [size]
  (let [rnd (new Random)
        sb (new StringBuffer)]
    (while (< (.length sb) size)
      (.append sb (Integer/toHexString (.nextInt rnd))))
    (-> sb .toString (.substring 0 size))))
