(ns mx.interware.arp.streams.common
  (:require [clojure.pprint :as pp]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.walk :refer [walk]]
            [clojure.core.async :as async :refer [chan go go-loop timeout <! >! <!! >!!]]
            [immutant.caching :as C]
            [mx.interware.arp.core.state :as ST])
            
  (:import 
    (java.net InetAddress URL)
    (org.apache.log4j PropertyConfigurator)
    (org.infinispan.configuration.cache ConfigurationBuilder)))

(defn exec-in [origin delta D-fn & args]
  (go
    (<! (timeout delta))
    (apply D-fn args)))

(defn repeat-every [origin delta count D-fn & args]
  (go-loop [n count]
    (when (or (> n 0) (= n -1))
      (<! (timeout delta))
      (apply D-fn args)
      (recur (if (> n 0) (dec n) n)))))

(defn add-attr [e k & params]
  (assoc e k (apply str params)))


(defmulti key-factory (fn [_ k] (coll? k)))

(defmethod key-factory false [by-path k]
  (if k 
    (vec (cons k (or by-path [])))
    by-path))

(defn complex-key-factory [by-path k-vec]
  (if k-vec 
    (into k-vec (or by-path []))
    by-path))

(defmethod key-factory true [by-path k-path]
  (let [by-path (or by-path [])
        by-sufix (fn [by-path idx]
                   (if-let [elem (get by-path idx)]
                     (str "(" elem ")")
                     ""))
        d-k (reduce 
              (fn [[path-str by-path-idx] path-element]
                (cond 
                  (= "by" path-element)
                  [(str path-str "/" path-element (by-sufix by-path by-path-idx)) (inc by-path-idx)]
                  
                  (= "join" path-element)
                  [(str path-str "/" path-element) (dec by-path-idx)]
                  
                  :OTHERWISE
                  [(str path-str "/" path-element) by-path-idx]))
              ["" 0]
              k-path)]
    (first d-k)))

(defn error 
  ([msg]
   (log/info msg))
  ([msg ex]
   (log/info msg)
   (.printStackTrace ex)))

(defn propagate [by-path state e children]
  (reduce 
    (fn [computed-state child]
      (try
        (child by-path computed-state e)
        (catch Throwable t
          (.printStackTrace t)
          (log/error "Error propagating event!" (pr-str e) (-> t .getClass .getName) (.getMessage t))
          computed-state))) 
    state 
    children))
 
(defn- compute-latency [{latency :arp/latency :as e}]
  (assoc e :arp/latency (- (System/nanoTime) latency)))

(defmulti mutate! (fn [state {arp-cmd :arp/cmd} & _] (or arp-cmd :send2streams)))

(defmethod mutate! :clear-all [state e & _]
  (try
    (let [e (compute-latency e)]
      (reduce
        (fn [result [k v]]
          (if (and (keyword? k) (= "arp" (namespace k)))
            (assoc result k v)
            result))
        (assoc state :arp/latency (:arp/latency e))
        state))
    (catch Throwable t
      (.printStackTrace t)
      state)))

(defmethod mutate! :clear-keys [state {:keys [filter-key-fn filter-key-re filter-key] :as e} & _]
  (try
    (let [e (compute-latency e)
          filter-fn (cond 
                     filter-key-fn
                     filter-key-fn
                   
                     filter-key-re
                     (fn [k]
                       (re-matches filter-key-re (str k)))
                   
                     filter-key
                     (fn [[k & _]]
                       (= k filter-key)))]
      (reduce 
        (fn [state [k v]]
          (if (and (coll? k) (filter-fn k))
            (dissoc state k)
            state))
        (assoc state :arp/latency (:arp/latency e))
        state))
    (catch Throwable t
      (.printStackTrace t)
      state)))

(defmethod mutate! :load-history [state {:keys [path now-millis date-fmt key-name latency] :as e} & _]
  (try
    (let [e (compute-latency e)
          file-name-filter (str 
                             (.format (java.text.SimpleDateFormat. date-fmt) 
                               (- (System/currentTimeMillis) now-millis)) 
                             "-" key-name)
          file-filter-re (re-pattern (str "(" file-name-filter ")\\-(.*)" "[\\.]edn"))
          filter (reify java.io.FilenameFilter
                   (^boolean accept [thsi ^java.io.File file ^String name]
                     (if (re-matches file-filter-re name)
                       true
                       false)))
          files (-> (java.io.File. path) (.listFiles filter))
          file-subscript (doall
                           (map 
                             (fn [f]
                               [f (clojure.string/split ((re-matches file-filter-re (.getName f)) 2) #"\\-")]) 
                             files))]
      (reduce
        (fn [state [d-file colofon]]
          (try
            (let [info (read-string (slurp d-file))
                  k (into [(keyword key-name)] colofon)]
              (println "se añade al state:" k info)
              (assoc state k info))
            (catch Throwable t
              (log/error 
                "Problem loading history: " 
                (.getAbsolutePath d-file) 
                (-> t .getClass .getName) (.getMessage t))
              state)))
        (assoc state :arp/latency (:arp/latency e))
        file-subscript))
    (catch Throwable t
      (.printStackTrace t)
      state)))

(defmethod mutate! :send2streams [state e & [streams & _]]

  (try
    (let [result (cond 
                   (map? e)
                   (streams [] state (compute-latency e))
    
                   :OTHERWISE
                   (do
                     (log/warn "Dropping invalid event: " (pr-str e))
                     state))]
      result)
    (catch Throwable t
      (.printStackTrace t)
      state)))

(defn create-sink [state streams]
  (let [sink (fn [e]
               (send state mutate! (assoc e :arp/latency (System/nanoTime)) streams))
        send2agent (fn [mutator-fn & args]
                     (apply send state mutator-fn args))]
    (send state assoc :arp/entry sink :arp/send2agent send2agent)
    (Thread/sleep 200)
    sink))
  

(defmulti start-listener (fn [sink config] (:type config)))

(defmethod start-listener :default [sink config]
  (log/error "Invalid listener : " (:type config)))

(defn arp-state-as-map [arp-agent]
  (let [state @arp-agent]
    (reduce 
      (fn [result [k v]]
        (if (and (keyword? k) (= "arp" (namespace k)))
          (dissoc result k)
          result))
      state
      state)))


(defn introduce-path [stateless stateful form]
  (let [all-streams (into stateless stateful)
        path (atom [])]
    (letfn [
            (trim-ns [e]
              (if-let [sym-ns (and (symbol? e) (namespace e))]
                (symbol (subs (str e) (inc (count (name sym-ns)))))
                e))
            (output [e] 
              (if (and (seq? e) 
                       (if (vector? (first e))
                         (all-streams (trim-ns (ffirst e)))
                         (all-streams (trim-ns (first e)))))
                (swap! path 
                  (fn [d-path]
                    (vec (butlast d-path)))))
              (if (and (seq? e) (vector? (first e)))
                (do
                  (let [[[sym path] & params] e]
                    (apply list sym path params)))
                e))
            (analyze [e] 
              (let [trimed-e (trim-ns e)]
                (if (seq? e)
                  (walk analyze output e)
                  (if (all-streams trimed-e)
                    (do
                      (swap! path conj (name trimed-e))
                      (if (stateful trimed-e)
                        [e @path]
                        e))
                    e))))]
      (walk analyze output form))))

(defn create-set-of-symbols [ns]
  (let [ns-coll (if (coll? ns) ns [ns])]
    (reduce (fn [result ns]
              (require (symbol ns))
              (into result (map first (ns-publics (symbol ns)))))
      #{}
      ns-coll)))

(defmacro using-path [stateless-ns-s stateful-ns-s form]
  (let [stateless (create-set-of-symbols stateless-ns-s)
        stateful (create-set-of-symbols stateful-ns-s)
        streams-with-path (introduce-path stateless stateful form)]
    (println :stateless stateless)
    (println :stateful stateful)
    (println)
    (println :using-path-entry-form)
    (pp/pprint form)
    (println)
    (println :using-path-modified-form)
    (pp/pprint streams-with-path)
    streams-with-path))

(defmacro using-path-with-default-streams [form]
  (using-path mx.interware.arp.streams.stateless mx.interware.arp.streams.stateful form))

(defmacro defstream [& params]
  (let [[name [params & form]] (if (symbol? (first params)) 
                                 [(first params) (rest params)] 
                                 [nil params])
        params (cond 
                 (nil? (seq params))
                 ['_by-path '_state '_event]
                 
                 (= (count params) 1)
                 (into ['_by-path '_state] params)
                 
                 (= (count params) 2)
                 (into ['_by-path] params)
                 
                 (= (count params) 3)
                 (vec params))]
    (if name
      `(fn ~name ~params (do ~@form ~(second params)))
      `(fn ~params (do ~@form ~(second params))))))



