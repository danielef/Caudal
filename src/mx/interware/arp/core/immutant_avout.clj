(ns mx.interware.arp.core.immutant-avout
  (:gen-class)
  (:require [immutant.caching :as C]
            [avout.state :as as])
  (:import 
    (java.net InetAddress URL)
    (org.apache.log4j PropertyConfigurator)))

(deftype ImmutantStateContainer [^org.infinispan.Cache cache node-id]
  
  as/StateContainer
  
  (initStateContainer [this])
    
  
  (destroyStateContainer [this]
    (.remove cache node-id))
  
  (getState [this]
    (.get cache node-id))
  
  (setState [this new-val]
    (C/swap-in! cache node-id (fn [_] new-val))))


(comment
  (def cache (C/cache "avout" :ttl 30000))
  (require '[avout.core :as A])
  (require '[avout.atoms :as AA])
  (require '[mx.interware.arp.core.immutant-avout :as IA])
  (defn im-atom [zk-client cache a-name] 
    (AA/distributed-atom zk-client a-name 
      (mx.interware.arp.core.immutant_avout.ImmutantStateContainer. cache a-name))))
      
