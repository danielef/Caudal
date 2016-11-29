(ns mx.interware.arp.streams.stateless
  (:require [clojure.pprint :as pp]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [clojure.core.async :as async :refer [chan go go-loop timeout <! >! <!! >!!]]
            [mx.interware.arp.core.state :as ST :refer [as-map lookup]]
            [mx.interware.arp.streams.common :refer [key-factory 
                                                     propagate error 
                                                     create-sink]])
  (:import (java.net InetAddress URL)
           (org.apache.log4j PropertyConfigurator)
           (org.apache.commons.io FileUtils)
           (org.infinispan.configuration.cache ConfigurationBuilder)))

(defn default 
  "
  Streamer function that sets a default value for a key of an event only if it does not exist or it does not have a value  
  > **Arguments:**  
     *key*: Key where the value will be set if it does not exist or it does not have a value  
     *value*: Default value to be assigned  
     *children*: Children streamer functions to be propagated
  "
  [key value & children]
  (fn [by-path state e]
    (propagate by-path state (assoc e key (key e value)) children)))

(defn with
  "
  Streamer function that sets a value for a key in an event  
  > **Arguments**:  
    *key*: Key where the value will be set  
    *value*: Value to be assigned to the key  
    *children*: Children streamer functions to be propagated  
  "
  [key value & children]
  (fn [by-path state e]
    (propagate by-path state (assoc e key value) children)))

(defn time-stampit
  "
  Streamer function that stores the current timestamp in milliseconds in an event  
  > **Arguments**:  
    *key*: key to store the current timestamp in milliseconds  
    *children*: Children streamer functions to be propagated  
  "
  [key & children]
  (fn [by-path state e]
    (propagate
      by-path 
      state
      (assoc e key (System/currentTimeMillis))
      children)))

(defn store! 
  "
  Streamer function that stores an event in state using a function for key names  
  > **Arguments**:  
    *id-fn*: Function to calculate key names  
    *children*: Children streamer functions to be propagated  
  "
  [id-fn & children]
  (fn [by-path state e]
    (let [state (if-let [id (id-fn e)]
                  (assoc state id e)
                  state)]
      (propagate by-path state e children))))

(defn where 
  "
  Streamer function that filters events using a conditional predicate  
  > **Arguments**:  
    *pred*: Conditional predicate to filter  
    *children*: Children streamer functions to be propagated
  "
  [pred & children]
  (fn [by-path state e]
    (if (pred e)
      (propagate by-path state e children)
      state)))
            
(defn smap 
  "
  Streamer function that applies a transformation function to events and propagate them  
  similarly to clojure's map function  
  > **Arguments**:  
    *fun*: Tranformation function  
    *children*: Children streamer functions to be propagated
  "
  [fun & children]
  (fn [by-path state e]
    (if-let [e (fun e)]
      (propagate by-path state e children)
      state)))

(defn split 
  "
  Streamer function that filters events by multiple conditionals  
  > **Arguments**:  
    *exprs*: Conditionals to filter  
  "
  [& exprs]
  (letfn [(pred [e [expr to-do]]
            (if-not to-do
              expr
              (if (expr e)
                to-do)))]
    (let [pairs (partition-all 2 exprs)]
      (fn [by-path state e]
        (if-let [to-do-fn (some (partial pred e) pairs)]
          (to-do-fn by-path state e)
          state)))))

(defn- filter-keys
  [key-set m]
  (if (= key-set :all)
    m
    (reduce 
      (fn [result [k v]]
        (assoc result k v)) 
      {} 
      (filter #(key-set (first %)) m))))

(defn unfold [& children]
  "
  Streamer function used for unfold event collections propagated by parent streamers. Events are therefore propagated one
  by one. If an individual event is received, it is propagated without any modification.
  > **Arguments:**
     *children*: Children streamer functions to be propagated
  "
  (fn stream [by-path state input]
    (cond (map? input)  (propagate by-path state input children)
          (coll? input) (reduce (fn [new-state event] (propagate by-path new-state event children)) state input)
          :else         (propagate by-path state input children))))

(defn to-file
  "
  Streamer function that stores events to a specified file
  > **Arguments**:
    *file-path*: The path to the target file
    *keys*: The keywords representing attributtes to be written in the file
    *children*: Children streamer functions to be propagated. Events are propagated untouched.
  "
  [file-path keys & children]
  (let [file (io/file file-path)]
    (fn stream [by-path state e]
      (let [new-event (filter-keys (set keys) e)]
        (FileUtils/writeStringToFile file (str (pr-str new-event) "\n") true)
        (propagate by-path state e children)))))

(defn ->DEBUG 
  "
   Streamer function that sends a DEBUG event to arp log  
   > **Arguments**:  
   *fields* : can be  
   - :all: then sends event to log with DEBUG level  
   - vector of keywords: send event with keys contained in vector to log with DEBUG level  
   - formatter-fn: a fn of arity 1 that the result will be the output to de log in DEBUG level  
   *children*: Children streamer functions to be propagated  
  " 
  [fields & children]
  (let [fields (if (or (= :all fields) (fn? fields)) 
                 fields 
                 (into #{} fields))]
    (fn [by-path state e]
      (let [e (if (fn? fields) 
                (fields e) 
                (pr-str (filter-keys fields e)))]
        (log/debug e))
      (propagate by-path state e children))))

(defn ->INFO 
  "
   Streamer function that sends a INFO event to arp log  
   > **Arguments**:  
   *fields* : can be  
   - :all: then sends event to log with INFO level  
   - vector of keywords: send event with keys contained in vector to log with DEBUG level  
   - formatter-fn: a fn of arity 1 that the result will be the output to de log in DEBUG level  
   *children*: Children streamer functions to be propagated  
  " 
  [fields & children]
  (let [fields (if (or (= :all fields) (fn? fields)) 
                 fields 
                 (into #{} fields))]
    (fn [by-path state e]
      (let [e (if (fn? fields) 
                (fields e) 
                (pr-str (filter-keys fields e)))]
        (log/info e))
      (propagate by-path state e children))))

(defn ->WARN
  "
   Streamer function that sends a WARN event to arp log  
   > **Arguments**:  
   *fields* : can be  
   - :all: then sends event to log with WARN level  
   - vector of keywords: send event with keys contained in vector to log with DEBUG level  
   - formatter-fn: a fn of arity 1 that the result will be the output to de log in DEBUG level  
   *children*: Children streamer functions to be propagated  
  "
  [fields & children]
  (let [fields (if (or (= :all fields) (fn? fields)) 
                 fields 
                 (into #{} fields))]
    (fn [by-path state e]
      (let [e (if (fn? fields) 
                (fields e) 
                (pr-str (filter-keys fields e)))]
        (log/warn e))
      (propagate by-path state e children))))

(defn ->ERROR 
   "
   Streamer function that sends a DEBUG event to arp log  
   > **Arguments**:  
   *fields* : can be  
   - :all: then sends event to log with ERROR level  
   - vector of keywords: send event with keys contained in vector to log with ERROR level  
   - formatter-fn: a fn of arity 1 that the result will be the output to de log in DEBUG level  
   *children*: Children streamer functions to be propagated  
  "
  [fields & children]
  (let [fields (if (or (= :all fields) (fn? fields)) 
                 fields 
                 (into #{} fields))]
    (fn [by-path state e]
      (let [e (if (fn? fields) 
                (fields e) 
                (pr-str (filter-keys fields e)))]
        (log/error e))
      (propagate by-path state e children))))               

(defn by
  "
  Streamer function that groups events by values of sent keys  
  > **Arguments**:  
    *fields*: Data keys to group by  
    *children*: Children streamer functions to be propagated
  "
  [fields & children]
  (let [fields (flatten fields)
        fld (if (= 1 (count fields))
              (first fields)
              (apply juxt fields))]
    (fn [by-path state e]
      (let [fork-name (fld e)]
        (propagate (into (or by-path []) (flatten [fork-name])) state e children)))))

(defn join
  "
  Streamer function that groups eliminates the efect of one 'by'  
  > **Arguments**:  
    *fields*: Data keys to group by  
    *children*: Children streamer functions to be propagated
  "
  [& children]
  (fn [by-path state e]
    (propagate (vec (butlast by-path)) state e children)))

(defn reinject
  "
   Streamer function that reinjects events to initial function
   in order to execute the whole proccess over it again.
  " 
  [& children]
  (fn [by-path {arp-entry :arp/entry :as state} e]
    (if arp-entry
      (arp-entry e))
    (propagate by-path state e children)))  

(defn decorate 
  "Streamer function that substracts information from state, merges it with the current event and then propagates the merged data  
  > **Arguments**:  
    *state-key-of-fn*: Can be: 
  - a function that when is applied to current event, returns the state key
  - a keyword, a vector is created with this key name that stores values from **by** streamers in execution path
  "
  [state-key-or-fn & children]
  (fn [by-path state e]
    (let [s-key (if (fn? state-key-or-fn)
                  (state-key-or-fn e)
                  (key-factory state state-key-or-fn))
          s (state s-key)]
      (propagate by-path state (merge e s) children))))

(defn anomaly-by-stdev 
  "Streamer function that propagates to children if metric-key > (avg + (stdev-factor * stdev))  
  > **Arguments**:  
    *stdev-factor*: a number representing who many sigmas to allow over avg
    *metric-key*: the key where the value to lookfor is inside the event
    *avg-key*: the key where the value of the average is inside the event
    *stdev-key*: the key where the value of the sigma is inside the event
  "
  [stdev-factor metric-key avg-key stdev-key & children]
  (fn [by-path state e]
    (let [metric (metric-key e)
          avg (avg-key e)
          stdev (stdev-key e)]
      (if (and (number? stdev-factor)
               (number? metric)
               (number? avg)
               (number? stdev)
               (> metric (+ avg (* stdev-factor stdev))))
        (propagate by-path state e children)
        state))))

(defn anomaly-by-percentil 
  "Streamer function that propagates to children if metric-key > (avg + (stdev-factor * stdev))  
  > **Arguments**:  
    *stdev-factor*: a number representing who many sigmas to allow over avg
    *metric-key*: the key where the value to lookfor is inside the event
    *avg-key*: the key where the value of the average is inside the event
    *stdev-key*: the key where the value of the sigma is inside the event
  "
  [metric-key percentiles-key percentil & children]
  (fn [by-path state e]
    (let [metric (metric-key e)
          percentil-value (get-in e [percentiles-key percentil])]
      (if-not percentil-value
        (log/warn (str "Percentil " percentil " not present in " percentiles-key " @ event " e)))
      (if (> metric percentil-value)
        (propagate by-path state e children)
        state))))

(defn forward
  "Streamer function that sends events to other ARP using TCP  
  > **Arguments**:  
    *arp-host*: host name or ip of ARP receiver  
    *port*: Port number on the ARP host enabled to listen for events  
    *children*: Streamers to propagate the unchanged event  
  "
  [arp-host arp-port & children]
  (let [socket-conf (agent {:host arp-host
                            :port arp-port})
        send-arp-events (fn [{:keys [host port socket] :as conf} events]
                          (try
                            (let [socket (or socket 
                                             (java.net.Socket. host port))
                                  out (.getOutputStream socket)]
                              (.write out (.getBytes (pr-str events) "utf-8"))
                              (.write out 10)
                              (.flush out)
                              (assoc conf :socket socket))
                            (catch Exception e
                              (.printStackTrace e)
                              (log/error e)
                              (dissoc conf :socket))))]
                              
    (fn [by-path state e]
      (send socket-conf send-arp-events e)
      (propagate by-path state e children))))
    
(defn percentiles 
  [metric-key percentiles-key percents & children]
  (fn [by-path state events]
    (let [events (filter (fn [{metric metric-key :as e}]
                           (and metric (number? metric))) events)]
      (if (seq events)
        (let [ordered (sort-by metric-key events)
              n (count ordered)
              idx-fn (fn [percent] (min (dec n) (Math/floor (* n percent))))
              values (into {} (map (fn [percent]
                                     [percent (metric-key (nth ordered (idx-fn percent)))]) percents))]
          (propagate by-path state (assoc (first events) percentiles-key values) children))
        state))))
       
    



