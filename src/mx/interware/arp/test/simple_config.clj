(ns mx.interware.arp.test.simple-config
  (:require [clojure.pprint :as pp]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.walk :refer [walk]]
            [clojure.core.async :as async :refer [chan go go-loop timeout <! >! <!! >!!]]
            [mx.interware.arp.core.state :refer [as-map lookup]]
            [mx.interware.arp.streams.common :refer [create-sink key-factory defstream
                                                     using-path using-path-with-default-streams]]
            [mx.interware.arp.streams.stateful  :refer [batch concurrent-meter counter 
                                                        changed log-matcher reduce-with
                                                        acum-stats dump-every
                                                        ewma-timeless rate]]
            [mx.interware.arp.streams.stateless :refer [->DEBUG ->INFO ->WARN ->ERROR
                                                        default where  smap split
                                                        with by reinject store! 
                                                        decorate anomaly-by-stdev forward
                                                        percentiles]]
            [mx.interware.arp.core.folds :as folds])
  (:import 
    (java.net InetAddress URL)
    (org.apache.log4j PropertyConfigurator)
    (org.infinispan.configuration.cache ConfigurationBuilder)))

(PropertyConfigurator/configure "log4j.properties")

(defn sprint [msg e]
  (println msg "->" (pr-str e)))

(def streams4
  (default :ttl -1
    (log-matcher ::log-matcher :init :end (juxt :tx :thread) 20000 :time
      prn
      (->INFO :all))))

(comment
  (-main "./config/arp-config.edn")
  {:tx "tx1" :thread "T1" :init true :time 1000}
  {:tx "tx1" :thread "T1" :init true :time 1001}
  {:tx "tx1" :thread "T1" :end true :time 5000}
  {:tx "tx1" :thread "T1" :end true :time 5000})


; calcular estadisticas de INX)

(defn mark [e]
  (assoc e :trace (java.lang.Exception. "TRACE")))

;187836

(def streams
  (default :ttl -1
    (where (fn [e] (= "getCustInfo" (:tx e)))
      (where :delta
        (rate :tx-rate :timestamp :rate 30 15
          (smap (fn [e]
                  (if (= "23:59" (.format (java.text.SimpleDateFormat. "HH:mm") (:timestamp e)))
                    (assoc e :rate (first (:rate e)))))
            (->INFO [:rate])
            (smap (fn [{:keys [rate] :as e}]
                    (assoc e :rate-stats (folds/simple-mean&stdev rate)))
              (->INFO [:rate-stats]))))
        (smap #(assoc % :delta (double (Integer/parseInt (:delta %))))
          (by [:tx]
            (batch :big 1000000 90000
              (percentiles :delta :percentiles [0 0.5 0.75 0.8 0.9 0.95]
                (smap (fn [e]
                        {:tx (:tx e) :percentiles (:percentiles e)})
                  (store! (fn [e] [:percentiles (:tx e)])
                    (dump-every :percentiles "hpercent" "yyyyMMdd" [1 :minute] "./config/stats/")))
                (->INFO [:tx :percentiles])))
            (batch :tx 1000 1000
              (smap #(folds/mean&std-dev :delta :avg :variance :stdev :n %)
                (acum-stats :stats :avg :variance :stdev :n
                  (dump-every :stats "history" "yyyyMM_ww" [1 :minute] "./config/stats/"))))
            (decorate :history
              (split 
                :avg 
                (anomaly-by-stdev 1 :delta :avg :stdev
                  (->ERROR :all))
                
                (ewma-timeless :ewma 0.5 :delta 
                  (decorate :stats
                    (anomaly-by-stdev 0 :delta :avg :stdev
                      (defstream [e] (println :despues-ewma e)))))))))))))
                      ;(->INFO :all))))))))))))
                      ;(forward "localhost" 7777))))))))))))
              
  
(def test-streams
  (defstream [e]
    (println "Efectivamente:" e)))
  
   

(comment defmacro using-path [streams]
  (clojure.pprint/pprint streams)
  ~@streams)

(def streams-w-paths
 (using-path mx.interware.arp.streams.stateless mx.interware.arp.streams.stateful
  (default :ttl -1
    (default :starting true 
      (where :tx
        (where :delta
          ;(smap (fn [e]
          ;        (println (.format (java.text.SimpleDateFormat. "yyyyMMdd-HH:mm:ss,SSS") (:timestamp e)))
          (rate :timestamp :rate 30 60
            (smap (fn [e]
                    (if (= "23:59" (.format (java.text.SimpleDateFormat. "HH:mm") (:timestamp e)))
                      (assoc e :rate (first (:rate e)))))
              (->ERROR [:rate])))
          (smap #(assoc % :delta (double (Integer/parseInt (:delta %))))
            (by [:tx]
              (batch 1000 1000
                (smap #(folds/mean&std-dev :delta :avg :variance :stdev :n %)
                  ;(->WARN [:tx :avg :variance :stdev :n :ttl])
                  (acum-stats :avg :variance :stdev :n
                    (smap (fn [{:keys [tx avg variance stdev n]}]
                            {:avg avg :variance variance :stdev stdev :n n :tx tx :ttl -1})
                      (store! (fn [{:keys [tx] :as e}]
                                [:stats tx])))
                    ;(dump-every "history" "yyyyMM_ww" [1 :minute] "./config/stats/")
                    (->WARN [:tx :avg :variance :stdev :n]))))
              (where :starting
               (ewma-timeless 0.5 :delta 
                (decorate (fn [{:keys [tx]}]
                            [:stats tx]) ;"../../../batch/smap/acum-stats"
                  (anomaly-by-stdev 2 :delta :avg :stdev
                    (->ERROR :all)
                    (forward "localhost" 7777)))))
              (decorate :history
                (anomaly-by-stdev 3 :delta :avg :stdev
                  (->ERROR :all)))))))))))

                
(def streams4
  (default :ttl -1
    (where :tx
      (->ERROR :all)
      (smap (fn [e]
              (assoc e :TS (:timestamp e)))
        (log-matcher ::log-matcher :start :delta (juxt :tx :thread) 30000 :timestamp
          (->WARN [:timestamp :thread :tx :delta :TS]
            (where #(= :timeout (:time-stamp %))
              (->WARN (fn [e] (str "****** TIMEOUT ***" e))))
            (where #(and (:delta %) (not= (str (:timestamp %)) (:delta %)))
              (->WARN (fn [e] (str "****** DIFERENCIA ***" e "==>" 
                                (and (:delta e) (not= (str (:timestamp e)) (:delta e))))))))))
      (by [:tx]
        (where #(and (:tx %) (:delta %))
          (counter ::cuenta :metric
            (->WARN #(str "DATA: " (:tx %) " " (:metric %)))))))))

(def streams3
  (changed ::changed :state prn))

(def streams2
  (default :ttl -1
    (changed ::changed :state
      (partial sprint "CAMBIO"))
    (concurrent-meter 
      ::txmeter 
      #(and (:tx %) (re-matches #".*>>1.*" (:msg %)))
      #(and (:tx %) (re-matches #".*>> [0-9]+.*" (:msg %)))
      :tx
      :cmeter
      (partial sprint "concurrent:"))
    (batch ::batcher 289 10000
      (partial sprint "BATCH")
      (smap (partial folds/mean :n)
        (partial sprint "MEAN"))
      (smap (partial folds/fold :n +)
        (partial sprint "SUM")))  
    (counter ::cuenta :metric
      (by [:app]
        (counter ::by-app :app-cnt
          (fn [e]
            (println :by-app (pr-str e)))))
      (where #(= "localhost" (:host %))
        (with :host "REINYECTADO"
          (with :ttl 10
            (reinject))))
      (fn [e]
        (println :SALIO (pr-str e))))))


