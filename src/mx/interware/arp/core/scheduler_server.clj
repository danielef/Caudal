(ns mx.interware.arp.core.scheduler-server
  (:require
    [clojure.pprint :as pp]        
    [clojure.java.io :as io]
    [clojure.tools.logging :as log]
    [clojure.string :as s]
    [clojure.core.async :refer [chan >!! alts! go-loop buffer]]
    [immutant.scheduling :refer [schedule in at every limit cron] :as S]        
    [mx.interware.arp.streams.common :refer [start-listener]]
    [mx.interware.arp.util.ns-util :refer [resolve&get-fn require-name-spaces]]))     

(defmethod start-listener 'mx.interware.arp.core.scheduler-server 
  [sink {:keys [jobs] :as config}]
  (doseq [{:keys [cron-def event-factory parameters]} jobs]
    (let [event-factory-ns (symbol (namespace event-factory))
          _                (require event-factory-ns)
          event-factory    (resolve event-factory)
          event-source     (event-factory parameters)]
      (schedule 
        (fn []
          (log/info "running schedule:" cron parameters)
          (sink (event-source)))
        (cron cron-def)))))
      
(defn state-admin-event-factory [{:keys [cmd] :as parameters}]
  (fn []
    (let [event (merge
                  {:arp/cmd cmd}
                  parameters)]
      event)))
