(ns mx.interware.arp.io.log4j-server
  (:require [clojure.tools.logging :as log]
            [clojure.pprint :as pp]
            [clojure.core.async :refer [chan >!! >! <! alts! go-loop]]
            [mx.interware.arp.streams.common :refer [start-listener]]
            [mx.interware.arp.util.ns-util :refer [resolve&get-fn require-name-spaces]])
  (:import (java.net InetSocketAddress ServerSocket Socket)
           (java.io ObjectInputStream BufferedInputStream DataInputStream)))           

(defn start-server [port message-parser sink]
  (let [sserv (ServerSocket. port)]
    (future
      (while (not (.isClosed sserv))
        (try
          (let [socket (.accept sserv)
                _ (println :despues-accept)
                ois (ObjectInputStream. (BufferedInputStream. (.getInputStream socket)))]
            (loop [obj (.readObject ois)]
              (let [{:keys [loggerName level threadName properties message timeStamp]} (bean obj)
                    parsed (when message-parser (message-parser message))
                    arp-e (merge
                            {:logger-name loggerName
                             :thead threadName
                             :level (.toString level)
                             :message message
                             :time-stamp timeStamp} 
                            (into {} (map (fn [[k v]] 
                                            [(keyword k) v]) properties)) 
                            parsed)]
                (sink arp-e)
                (recur (.readObject ois)))))
          (catch Exception e
            (.printStackTrace e)))))
    sserv))
           

(defmethod start-listener 'mx.interware.arp.io.log4j-server 
  [sink config]
  (let [{:keys [port parser]} (get-in config [:parameters])
        parser-fn (if parser (resolve&get-fn parser))]
    (start-server port parser-fn sink)))

