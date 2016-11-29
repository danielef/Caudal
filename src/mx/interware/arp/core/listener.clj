(ns mx.interware.arp.core.listener
  (:gen-class)
  (:require [clojure.pprint :as pp]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [immutant.caching :as C]
            [mx.interware.arp.core.state :as ST])
  (:import 
    (java.net InetAddress URL)
    (org.apache.log4j PropertyConfigurator)
    (org.infinispan.configuration.cache ConfigurationBuilder)
    (org.infinispan.notifications.cachelistener.annotation 
      CacheEntryActivated CacheEntryCreated 
      CacheEntryEvicted CacheEntryInvalidated 
      CacheEntryLoaded CacheEntryModified 
      CacheEntryPassivated CacheEntryRemoved 
      CacheEntryVisited)
    
    (org.infinispan.notifications Listener)))
(definterface EventListener 
  (^void event [e]))

(deftype ^{Listener true} InfListener []
  EventListener 
  (^{CacheEntryActivated true 
     CacheEntryCreated true
     CacheEntryEvicted true
     CacheEntryInvalidated true
     CacheEntryLoaded true
     CacheEntryModified true
     CacheEntryPassivated true
     CacheEntryRemoved true
     CacheEntryVisited true}
    ^void event [this e] (println :listener---2 e)))
  
