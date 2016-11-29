(ns mx.interware.arp.util.matrix)

(defn m-size [m]
  [(count m) (count (first m))])

(defn m-zero [r c]
  (vec (repeat r (vec (repeat c 0)))))

(defn m-get [m r c]
  (-> m (get r) (get c)))

(defn m-set [m r c v] 
  (let [R (get m r)] 
    (assoc m r (assoc R c v))))

(defn m-inc [m r c]
  (try
    (m-set m r c (inc (m-get m r c)))
    (catch Exception e
      (.printStackTrace e)
      (println :m m :r r :c c)
      (System/exit 0))))

(defn m-insert-zero-row [m]
  (let [[r c] (m-size m)]
    (vec (butlast (cons (vec (repeat c 0)) m)))))
