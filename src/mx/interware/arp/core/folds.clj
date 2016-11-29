(ns mx.interware.arp.core.folds)

(defn non-nil-fld [fld-key events]
  (filter #(fld-key %) events))

(defn with-numeric-fld [fld-key events]
  (filter #(number? (fld-key %)) events))

(defn fold
  ([fld-key reduction-fn events]
   (let [[e & events :as all-events] (non-nil-fld fld-key events)]
     (if (and e (seq events))
       (assoc e fld-key (reduce reduction-fn (map fld-key all-events)))
       e)))
  ([reduction-fn events]
   (fold :metric reduction-fn events)))

(defn- avg [nums]
  (/ (reduce + nums) (count nums)))

(defn mean 
  ([fld-key events]
   (let [[e & events :as all-events] (with-numeric-fld fld-key events)]
     (if (and e (seq events))
       (assoc e fld-key (/ (reduce + (map fld-key all-events)) (count all-events)))
       e)))
  ([events]
   (mean :metric events)))

(defn simple-mean&stdev [metrics]
  (let [mean (double (avg metrics))
        sqr-avg-x (Math/pow mean 2)
        avg-sqr-x (double (avg (map #(* % %) metrics)))
        n (count metrics)
        variance (- avg-sqr-x sqr-avg-x)
        stdev (Math/sqrt variance)]
    [mean stdev variance n]))
  

(defn mean&std-dev 
  "Este fold revibe un vector de eventos, selecciona los eventos cuyo 'metric-key'
  es numérico, luego calcula el promedio y la desvuación estandar, regresa el primer evento
  de la coleccion'events' asociando 'mean-key' con el promedio, 'stdev-key' con la desviación
  estandar y 'count-key' con el número de eventos con métrica numérica"
  [metric-key mean-key variance-key stdev-key count-key events]
  (let [
        metrics (vec (map metric-key (with-numeric-fld metric-key events)))
        mean (avg metrics)
        sqr-avg-x (Math/pow mean 2)
        avg-sqr-x (avg (map #(* % %) metrics))
        n (count metrics)
        
        variance (- avg-sqr-x sqr-avg-x)
        stdev (Math/sqrt variance)]
    (assoc (first events) mean-key mean variance-key variance stdev-key stdev count-key n)))

(defn intern-apply-fn
  ([apply-fn fld-key events]
   (println :fld-key fld-key)
   (let [[e & events :as all-events] (with-numeric-fld fld-key events)]
     (if (and e (seq events))
       (assoc e fld-key (apply apply-fn (map fld-key all-events)))
       e)))
  ([apply-fn events]
   (intern-apply-fn apply-fn :metric events)))

(defn max 
  ([fld-key events]
   (intern-apply-fn max fld-key events))
  ([events]
   (intern-apply-fn max events)))

(defn min 
  ([fld-key events]
   (intern-apply-fn min fld-key events))
  ([events]
   (intern-apply-fn min events)))

(defn sum
  ([fld-key events]
   (intern-apply-fn + fld-key events))
  ([events]
   (intern-apply-fn + events)))

(comment
  (def T (vec (map (fn [n] (if (= 4 n) {:m n} {:metric n :m n})) (range 1 6))))  
  (fold :m + T)
  (fold + T)
  (mean T)
  (max :m T)
  (min T)
  (sum T))
  

