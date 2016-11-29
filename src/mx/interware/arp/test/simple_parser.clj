(ns mx.interware.arp.test.simple-parser)

(defn parse-log-line [line]
  (println "line : " line)
  (read-string line))
  

(defn parse-cathel-msg [message]
  (let [re #".*(EJECUTANDO) +([a-zA-Z0-9]+).*time .>>.*|.*(FINALIZANDO) +([a-zA-Z0-9]+).*time .>> +([0-9]+).*"
        [_ start tx1 end tx2 delta] (re-matches re message)]
    (cond 
      start
      {:tx tx1 :start true}
      
      end
      {:tx tx2 :delta delta})))

(def contador (atom 0))

(defn parse-cathel-line [message]
  (swap! contador (fn [n] 
                    (let [n (inc n)] 
                      (if (= 0 (mod n 10000)) 
                        (println n)) 
                      n)))
  (let [re #"([0-9]{2}):([0-9]{2}):([0-9]{2}),([0-9]{1,3}) ([A-Z]+).*:[0-9]+-([0-9]+)\) (.*)"
        [_ HH mm ss SSS level thread msg :as all] (re-matches re message)]
    (when all
      (let [SSS (if (< (count SSS) 3)
                  (str SSS "0")
                  SSS)
            SSS (if (< (count SSS) 3)
                  (str SSS "0")
                  SSS)
            msg-info (if msg (parse-cathel-msg msg))
            cal (doto 
                  (java.util.Calendar/getInstance)
                  (.set java.util.Calendar/HOUR_OF_DAY (java.lang.Integer/parseInt HH))
                  (.set java.util.Calendar/MINUTE (java.lang.Integer/parseInt mm))
                  (.set java.util.Calendar/SECOND (java.lang.Integer/parseInt ss))
                  (.set java.util.Calendar/MILLISECOND (java.lang.Integer/parseInt SSS)))]
        (merge
          {:thread (str "thread-" thread)
           :level level
           :message msg
           :timestamp (.getTimeInMillis cal)}
          msg-info)))))  


