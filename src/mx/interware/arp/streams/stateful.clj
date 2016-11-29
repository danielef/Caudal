(ns mx.interware.arp.streams.stateful
  (:require [clojure.pprint :as pp]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.core.async :as async :refer [chan go go-loop timeout <! >! <!! >!!]]
            ;[immutant.caching :as C]
            [mx.interware.arp.core.state :as ST]
            [mx.interware.arp.util.matrix :as M]
            ;[mx.interware.arp.core.atom-state]
            ;[mx.interware.arp.core.immutant-state]
            [mx.interware.arp.util.date-util :refer [cycle->millis MILLIS-IN-DAY TZ-OFFSET
                                                     compute-rate-bucket]]
            [mx.interware.arp.streams.common :refer [key-factory complex-key-factory
                                                     propagate error 
                                                     create-sink 
                                                     exec-in repeat-every add-attr]])
  (:import 
    (java.net InetAddress URL)
    (org.apache.log4j PropertyConfigurator)
    (org.infinispan.configuration.cache ConfigurationBuilder)))


(defn counter 
  "
  Streamer function that appends to an event the count of passed events  
  > **Arguments**:  
    *state-key*: State key to store the count  
    *event-counter-key*: Event key to hold the count  
    *children*: Children streamer functions to be propagated  
  "
  [state-key event-counter-key & children]    
  (letfn [(mutator [{:keys [n ttl] :or {n 0 ttl -1}} e]
            {:n (inc n) :ttl ttl})]
    (fn [by-path state e]
      (let [d-k (key-factory by-path state-key)
            {{:keys [n ttl]} d-k :as new-state} (update state d-k mutator e)
            result (propagate
                     by-path 
                     new-state
                     (assoc e event-counter-key n)
                     children)]
        result))))

(defn ewma-timeless
  "
  Streamer function that normalizes the metric value, it calculates the ewma
  which represents the exponential waited moving average and is calculated by the next equation:   
  (last-metric * r) + (metric-history * (1-r)), events with no metric will be ignored  
  > **Arguments**:  
  *state-key*: state key to store the metric  
  *r*: Ratio value  
  *(metric-key)*: Optional for key holding metric, default :metric
  *children* the streamer functions to propagate  
  "
  [state-key r & [metric-key & children :as all-children]]
  (let [[metric-key children] (if (keyword? metric-key) [metric-key children] [:metric all-children])]
    (letfn [(mutator [{:keys [ewma ttl] :or {ttl -1}} e-metric]
              (if ewma
                {:ewma (+ (* r e-metric) (* (- 1 r) ewma)) :ttl ttl}
                {:ewma e-metric :ttl ttl}))]
      (fn [by-path state e]
        (if-let [e-metric (metric-key e)]
          (let [d-k (key-factory by-path state-key)
                {{:keys [ewma]} d-k :as new-state} (update state d-k mutator e-metric)]
            (propagate
              by-path 
              new-state
              (assoc e metric-key ewma)
              children))
          state)))))
        
  
(defn reduce-with
  "
  Streamer function that reduces values of an event using a function and propagates it
  > **Arguments**:  
  *state-key*: Key name to reduce  
  *fun*: function that reduces the values  
  *children*: Children streamer functions to be propagated  
  "
  [state-key fun & children]
  (fn [by-path state e]
    (let [d-k (key-factory by-path state-key) 
          {reduced d-k :as new-state} (update state d-k fun e)]
      (if reduced
        (propagate by-path new-state reduced children)
        new-state))))

(defn moving-time-window
  "
  Streamer function that propagate events to a vector of streamers using a moving time window delay  
  > **Arguments**:  
  *state-key*: State key   
  *n*: Max number of events  
  *(time-key)*: Optional key holding the time i millis, default :time
  *& children*: functions to propagate  
  "
  [state-key n & [time-key & children :as all-children]]
  (let [[time-key children] (if (keyword? time-key) [time-key children] [:time all-children])]
    (letfn [(mutator [{:keys [boundary buffer] :or {boundary 0 buffer []}} e]
              (let [boundary (max (- (time-key e 0) n) boundary)]
                (when (or (nil? (time-key e))
                          (< boundary (time-key e)))
                  (let [buffer (conj buffer e)]
                    {:boundary boundary
                     :buffer (vec (filter #(or (nil? (time-key %)) (< boundary (time-key %))) buffer))}))))]
      (fn [by-path state e]
        (let [d-k (key-factory by-path state-key)
              {{:keys [buffer]} d-k :as new-state} (update state d-k mutator e)]
          (if (and buffer (seq buffer))
            (propagate by-path new-state buffer children)
            new-state))))))

(defn changed
  "
  Streamer function that checks if a value of an event has changed using a predicate  
  > **Arguments**:  
    *state-key*: State key to store  
    *change-pred*: predicate for evaluating   
    *children*: Children streamer functions to be propagated  
  "
  [state-key change-pred & children]
  (letfn [(mutator [{:keys [e-t-1 val-t-1]} e]
            (let [val-t (change-pred e)
                  ret-val {:e-t-1 e :val-t-1 val-t}]
              (if-let [changed? (and e-t-1 (not= val-t-1 val-t))]
                (merge ret-val {:changed? true :old e-t-1})
                ret-val)))]
    (fn [by-path state e]
      (let [d-k (key-factory by-path state-key)
            {{:keys [changed? old]} d-k :as new-state} (update state d-k mutator e)]
        (if changed?
          (propagate by-path new-state [old e] children)
          new-state)))))
            
(defn rollup
  "
  Streamer function that propagates n events each dt time  
  > **Arguments**:  
  *state-key*: Key name to roll back  
  *n*: Max number of elements stored  
  *dt*: Max time to perform operations
  *children*: Children streamer functions to be propagated  
  "
  [state-key n dt & children]
  (letfn [(mutator [{:keys [cnt init-ts buf ttl] :or {cnt 0 buf [] ttl -1}} e]
            (let [now (System/currentTimeMillis)
                  init-ts (or init-ts now)]
              (cond
                (nil? e)
                {:rolled-up buf}
                (= cnt 0)
                {:cnt 1 :ts now :buf [] :send-it [e] :start-timer true}
                (< cnt (dec n))
                {:cnt (inc cnt) :ts init-ts :buf [] :send-it [e]}
                :OTHERWISE
                {:cnt (inc n) :ts init-ts :buf (conj buf e)})))
          (rollup-fn [state by-path d-k]
            (let [{{:keys [rolled-up]} d-k :as new-state} (update state d-k mutator nil)]
              (if (seq rolled-up)
                (propagate
                  by-path 
                  new-state
                  rolled-up
                  children))
              new-state))]
    (fn [by-path state e]
      (let [d-k (key-factory by-path state-key)
            {{:keys [send-it start-timer]} d-k 
             send2agent :arp/send2agent
             :as new-state} (update state d-k mutator e)]
        (when start-timer
          (exec-in :rollup dt send2agent rollup-fn by-path d-k))
        (if send-it
          (propagate by-path new-state send-it children)
          new-state)))))
                
(defn batch
  "
  Streamer function that propagates a vector of events once n elements are stored in or dt time has passed   
  > **Arguments**:  
  *state-key*: Key name to store  
  *n*: Max number of elements stored  
  *dt*: Max time to propagate  
  *children*: Children streamer functions to be propagated  
  "
  [state-key n dt & children]
  (letfn [(mutator [{:keys [buffer creation-time] :or {buffer []}} e]
            (let [start-timer   (not creation-time)
                  creation-time (or creation-time (System/currentTimeMillis))
                  buffer        (if e (conj buffer e) buffer)
                  flush-it?     (or (>= (count buffer) n) 
                                    (< (+ creation-time dt) 
                                       (System/currentTimeMillis))
                                    (nil? e))]
              (if flush-it?
                {:flushed buffer}
                {:buffer buffer :creation-time creation-time :ttl -1 :start-timer? start-timer})))
          (batcher-fn [state by-path d-k]
            (let [{{:keys [flushed]} d-k :as new-state} (update state d-k mutator nil)]
              (if (seq flushed)
                (propagate by-path new-state flushed children)
                new-state)))]
    (fn [by-path state e]
      (let [d-k (key-factory by-path state-key)
            {{:keys [flushed start-timer?]} d-k 
             send2agent :arp/send2agent
             :as new-state} (update state d-k mutator e)]
        (if start-timer?
          (exec-in :batch dt send2agent batcher-fn by-path d-k))
        (if (and flushed (seq flushed))
          (propagate by-path new-state flushed children)
          new-state)))))

(defn concurrent-meter
  "
  Stream function that stores in event the current number of transactions opened  
  when an initial event passes, the counter increments, if an end event paseses, the counter decrements  
  > **Arguments**:  
  *state-key*: State key name to store the counter  
  *init-tx-pred*: Predicate to detect the initial event of an operation  
  *init-tx-end*: Predicate to detect the finish event of an operation  
  *tx-id-fn: Function to get the transaction id  
  *[]: list that contains:
  - event key name to store the meter  
  - children functions to propagate
  "
  [state-key init-tx-pred end-tx-pred tx-id-fn & 
                        [concurrent-fld-cntr & children :as all-children]]
  (let [[metric-key children] (if (keyword? concurrent-fld-cntr) 
                                [concurrent-fld-cntr  children] 
                                [:metric all-children])]
    (letfn [(mutator [{:keys [n] :or {n 0} :as state} e]
              (cond 
                (init-tx-pred e)
                {:n (inc n) :propagate? true}
                
                (end-tx-pred e)
                {:n (dec n) :propagate? true}
              
                :OTHERWISE
                (dissoc state :propagate?)))]
      (fn [by-path state e]
        (if-let [id (tx-id-fn e)]
          (let [d-k (complex-key-factory by-path [state-key id])
                {{:keys [n propagate?]} d-k :as new-state} (update state d-k mutator e)]
            (if propagate?
              (propagate by-path new-state (assoc e metric-key n) children)
              new-state))
          state)))))
    

(defn log-matcher
  "
  Stream function that correlates initial and end events, extract the metric value and calculates the metric difference  
  > **Arguments**:  
  *state-key*: Key name to store the time  
  *init-tx-pred*: Predicate to detect the initial event of an operation  
  *init-tx-end*: Predicate to detect the finish event of an operation  
  *tx-id-fn*: Function to get the transaction id  
  *timeout-delta*: Time to wait the end event in milliseconds, when timeout, it will discard the event  
  *[]*:List that contains the key name to store the difference miliseconds and childs to propagate 
  "
  [state-key init-pred end-pred tx-id-fn timeout-delta & 
                   [ts-key & children :as all-children]]
  (let [[ts-key children] (if (keyword? ts-key) 
                            [ts-key  children] 
                            [:time all-children])]
    (letfn [(mutator [{:keys [start] :or {} :as state} e type init? end?]
              (cond 
                (= type :timeout)
                (if start
                  (if (= (ts-key e) start)
                    {:timeout? true}
                    {:start start :ttl -1})
                  {})
                
                init?
                {:start (ts-key e) :ttl -1}
                
                end?
                (if start
                  {:propagate-metric? (- (ts-key e) start) :ttl -1}
                  {})))
            (matcher-fn [state by-path d-k e]
              (let [{{:keys [timeout?]} d-k :as new-state} (update 
                                                             state d-k mutator 
                                                             e :timeout nil nil)]
                (if timeout?
                  (propagate by-path new-state (assoc e ts-key :timeout) children)
                  new-state)))]
      (fn [by-path state e]
        (let [id (tx-id-fn e)
              init? (init-pred e)
              end? (end-pred e)]
          ;(println :log-matcher1 id init? end? e)
          (if (and id (or init? end?))
            (let [d-k (complex-key-factory by-path [state-key id])
                  {{:keys [start propagate-metric?]} d-k 
                   send2agent :arp/send2agent
                   :as new-state} (update  state d-k mutator e :normal init? end?)]
              (when start
                (exec-in :log-matcher timeout-delta send2agent matcher-fn by-path d-k e))
              (if propagate-metric?
                (propagate by-path new-state (assoc e ts-key propagate-metric?) children)
                new-state))
            state))))))

(defn acum-stats 
  "
  Stream function that acumulates stats (mean,variance,stdev,count) recomputing them, it receives an event
  that is decorated with mean,stdev,variance and count and takes information from the state and the
  resultin mixing is stored in the state, then the event with the new values is propagated to children
  > **Arguments**:  
  *state-key*: Key name to store accumulating mean,stdev,variance and count
  *avg-key*: Key where the mean is in the event and in the state  
  *variance-key*: Key where the variance is in the event and in the state  
  *stdev-key*: Key where the stdev is in the event and in the state  
  *count-key*: Key where the count is in the event and in the state  
  *children*: List of streams to propagate result  
  -- if event is missign required fields, the event is propagated with :error set to message error  
  "
  [state-key avg-key variance-key stdev-key count-key & children]
  (letfn [(mutator [{avg avg-key variance variance-key stdev stdev-key n count-key :as stats} e]
            (if stats
              (let [block-mean (avg-key e)
                    block-variance (variance-key e)
                    block-stdev (stdev-key e)
                    block-count (count-key e)
                    total (+ n block-count)
                    new-variance (/ (+ (* variance n) (* block-variance block-count)) total)]
                (merge 
                  stats
                  {count-key total
                   avg-key (/ (+ (* avg n) (* block-mean block-count)) total)
                   variance-key new-variance
                   stdev-key (Math/sqrt new-variance)
                   :ttl -1}))
              {count-key (count-key e)
               avg-key (avg-key e)
               variance-key (variance-key e)
               stdev-key (stdev-key e)
               :ttl -1}))]
    (fn [by-path state {avg avg-key variance variance-key stdev stdev-key count count-key :as e}]
      (if (and (number? avg)
               (number? variance)
               (number? stdev)
               (number? count))
        (let [d-k (key-factory by-path state-key)
              {new-stats d-k :as new-state} (update state d-k mutator e)
              decorated-e (merge e new-stats {:d-k d-k :e e})]
          (propagate by-path new-state decorated-e children))
        (propagate by-path state (add-attr e :error 
                                   "event with missing on non-numeric fields:"
                                   avg-key variance-key stdev-key count-key)
          children)))))

(defn dump-every 
  "
  Stream function that writes to the file system information stored y the state.  
  > **Arguments**:  
  *state-key*: Key of the state to write to the file system  
  *file-name-prefix*, *date-format* and *dir-path*: are used to copute the file name like this:  
  - Arquimedes take the JVM time and formats it using a java.text.SimpleDateFormat with *date-format* and
  - concatenates it with '-' and file-name-prefix, then appends to it the current *'by string list'* then
  - appends '.edn' the file will be located at the *dir-path* directory.  
  *cycle*: represents a long number for milliseconds or a vetor with a number ant time messure like:  
  - [5 :minutes] or [1 :day]  
  - *cycle* will thel arquimedes to write this file width this frequency posibly overwriting it.  
  "
  [state-key file-name-prefix date-format cycle dir-path]
  (letfn [(mutator [{:keys [dumping?] :as stats} e]
            (if dumping?
              (dissoc stats :start-dumping?)
              (assoc stats :dumping? true :start-dumping? true)))
          (dumper-fn [state by-path d-k]
            (if-let [dump-info (state d-k)]
              (let [date-str (.format (java.text.SimpleDateFormat. date-format) (java.util.Date.))
                    ; dump-name looks like 
                    ; asuming format:'yyyyMM_ww' and file-name-prefix:'history'
                    ;    201601_03-history-getCustInfo.edn
                    dump-name (apply str date-str (map (fn [a b] (str a (name b))) 
                                                    (repeat "-") 
                                                    (cons file-name-prefix (rest d-k))))
                    dump-name (str dump-name ".edn")
                    dump-dir (java.io.File. dir-path)
                    dump-file (java.io.File. dump-dir dump-name)]
                (with-open [out (java.io.FileWriter. dump-file)]
                  (binding [*out* out]
                    (pp/pprint (dissoc dump-info :dumping? :start-dumping?))))))
            state)]
    (fn [by-path state e]
      (let [d-k (key-factory by-path state-key)
            {{:keys [start-dumping?]} d-k 
             send2agent :arp/send2agent
             :as new-state} (update state d-k mutator e)]
        (if start-dumping?
          (repeat-every :dump-every (cycle->millis cycle) -1 send2agent dumper-fn by-path d-k))
        new-state))))

;:tx-rate :timestamp :rate 30 60
(defn rate
  "
  Stream function that matains a matrix fo *days* rows and 60*24 columns, and stores it in the 
  state, then for each event that passes through increments the number in the position of row
  0 and collum equals to the number of minute in the day, remainder rows contain past *days*
  minus 1 (days shold be less than 60 '2 months').  
  > **Arguments**:  
  *state-key*: Key of the state to hold the matrix  
  *ts-key*: keyword of the timestamp in millis  
  *rate-key*: keyword holding the matrix when the outgoing event is propagated to *children*  
  *days*: Number of days to hold (number of rows in the matrix) 
  *minutes2group*: number of minutes to group, larger numbers require less memory (ex: 15 means we know
  how many events are dividing the hour in 4 (15 minutes) it is required that this number divides 60 with 
  no fraction(MEJORAR).  
  *children*: List of streams to propagate result  
  -- if event dosent have 'ts-key' event is ignored!!  
  "
  [state-key ts-key rate-key days bucket-size & children]
  (letfn [(mutator [{:keys [matrix last] :as data} e]
            ;(println :matrix2 matrix :last last)
            (if (ts-key e)
              (let [[day hour bucket] (compute-rate-bucket (ts-key e) bucket-size)
                    bucketsXhour (int (/ 60 bucket-size))
                    collumns (* bucketsXhour 24)
                    collumn (+ (* hour bucketsXhour) bucket)]
                ;(println :day day :hour hour :bucket bucket :collumns collumns :column collumn (java.util.Date. (+ (ts-key e) TZ-OFFSET)))
                (if data
                  (let [matrix (cond
                                 (or (< day last)
                                     (> (- day last) days))
                                 (M/m-zero days collumns)
                                                                  
                                 :OTHERWISE
                                 (loop [matrix matrix adjust (- day last)]
                                   (if (zero? adjust)
                                     matrix
                                     (recur (M/m-insert-zero-row matrix) (dec adjust)))))]
                    (do
                      ;(println "**********************")
                      ;(println :matrix matrix)
                      ;(println :bucket bucket :day day)
                      {:matrix (M/m-inc matrix 0 collumn)
                       :last day
                       :ttl -1}))
                  {:last day
                   :matrix (M/m-inc (M/m-zero days collumns) 0 collumn)
                   :ttl -1}))
              data))]
    (fn [by-path state e]
      ;(println :rateing e)
      (let [d-k (key-factory by-path state-key)
            {{:keys [matrix]} d-k :as new-state} (update state d-k mutator e)]
        (if matrix
          (propagate by-path new-state (assoc e rate-key matrix) children)
          new-state)))))
      
(defn mixer [state-key delay ts-key priority-fn & children]
  (letfn [(decision-fn [frontier [ts priority nano event]] ; buffer contains vectors like:[ts priority nano event]
            (< nano frontier)) 
          
          (sortable-event [e]
            [(ts-key e) (priority-fn e) (System/nanoTime) e])
            
          (mutator [{:keys [buffer] :as data} e]
            (if buffer
              (let [limit (- (System/currentTimeMillis) delay)
                    ;should-purge? is a fn that is true when a ts (first in sortable-event) is older than limit
                    should-purge? (fn [[ts]] (< ts limit))
                    buffer (if e (conj buffer (sortable-event e)) buffer)]
                (if (some should-purge? buffer)
                  (let [buffer (sort buffer)
                        older (partial decision-fn (- (System/nanoTime) (* delay 1000000)))
                        purged (seq (map #(get % 3) (take-while older buffer)))
                        keep (seq (drop-while older buffer))]
                    {:buffer keep :purged-events purged :ttl (if keep -1 0)})
                  {:buffer buffer :ttl -1}))
              (let [buffer (if e [(sortable-event e)])]
                {:buffer buffer
                 :start-purging? e
                 :ttl (if (or e buffer) -1 0)})))
          (cleanup-fn [state by-path d-k delay]
            (let [{{:keys [purged-events buffer]} d-k 
                   send2agent :arp/send2agent
                   :as new-state} (update state d-k mutator nil)
                  new-state (if (seq purged-events)
                              (propagate by-path new-state purged-events children)
                              new-state)]
              (if buffer
                (exec-in :mixer2 delay send2agent cleanup-fn by-path d-k delay))
              new-state))]
    (fn [by-path state e]
      ;(pp/pprint (ST/as-map state))
      (let [d-k (key-factory by-path state-key)
            {{:keys [start-purging? purged-events]} d-k
             send2agent :arp/send2agent
             :as new-state} (update state d-k mutator e)]
        (when start-purging?
          (exec-in :mixer1 delay send2agent cleanup-fn by-path d-k delay))
        (if (seq purged-events)
          (propagate by-path new-state purged-events)
          new-state)))))
              


