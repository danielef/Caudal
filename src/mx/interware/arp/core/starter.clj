(ns mx.interware.arp.core.starter
  (:gen-class
    :name "mx.interware.arp.core.Starter")
  (:require [clojure.tools.logging :as log]
            [clojure.pprint :as pp]
            [clojure.string :as string]
            [clojure.core.async :as async :refer [chan go go-loop timeout <! >! <!! >!! mult tap]]
            [mx.interware.arp.streams.common :refer [start-listener create-sink]]
            [mx.interware.arp.util.ns-util :refer [resolve&get-fn require-name-spaces]])
  (:import  (org.apache.log4j PropertyConfigurator)))

(PropertyConfigurator/configure "log4j.properties")

(defn -main [& [conf-file-path & args]]
  (let [env-config-str   (if conf-file-path
                           (slurp conf-file-path)
                           (System/getenv "ARP_CONFIG"))
        arp-config-str   (if env-config-str
                           env-config-str
                           (System/getProperty "ARP_CONFIG"))
        config           (read-string arp-config-str)
        _                (do 
                           (println "ARP configuration : ")
                           (pp/pprint  config))
        listeners        (get-in config [:arp :listeners])
        sink-map         (reduce 
                           (fn [sink-map [stream-k {:keys [origin]}]]
                             (println (str "========== origin ========>" origin))
                             (try
                               (let [origin-ns        (symbol (namespace origin))
                                     _                (require origin-ns)
                                     streams (resolve origin)
                                     state   (agent {}
                                                    :validator (fn [agt]
                                                                 (map? agt))
                                                    :error-handler (fn [agt ex]
                                                                     (.printStackTrace
                                                                       (java.lang.Exception. "MALO"))
                                                                     (.printStackTrace ex)))
                                     sink    (create-sink state streams)]
                                 (assoc sink-map stream-k [sink state]))
                               (catch Exception e
                                 (do
                                   (.printStackTrace e)
                                   (log/info (str "Cannot load streams [" origin "]. Reason : " (.getMessage e)))
                                   sink-map))))
                           {}
                           (get-in config [:arp :streams]))]
    (doseq [{:keys [type stream-to] :as listener} listeners]
      (try
        (println (str "========== type ========>" type))
        (let [_ (require type)
              sinks (map second
                         (filter
                           (fn [[sink-k [sink state]]]
                             (some (fn [stream-to] (= sink-k stream-to)) stream-to))
                           sink-map))
              D-sink (reduce comp (map first sinks))]
          (start-listener D-sink listener))
        (catch Exception e
            (log/info (str "Cannot start listener [" listener "]. Reason : " (.getMessage e))))))))
