(ns mx.interware.arp.io.log-feed-simulator
  (:require [clojure.java.io :as io]
            [mx.interware.arp.test.simple-parser :refer [parse-cathel-msg]])
  (:import (java.net InetSocketAddress Socket)
           (java.io PrintStream BufferedOutputStream)))

(def LOG-LINE-TEMPLATE 
  #"([0-9]{2}:[0-9]{2}:[0-9]{2}),([0-9]{3}) INFO  \[stdout\] \(ajp\-\/0.0.0.0:8209-([0-9]{1,3})\) (.*)")

(defn calc-wait [last-ref hora milli]
  (let [sdf (java.text.SimpleDateFormat. "HH:mm:ss.SSSZZZZZ")
        str-date (str hora "." milli "+0000")
        last (or last-ref (.parse sdf str-date))
        next (.parse sdf str-date)]
    [next (- (.getTime next) (.getTime last))]))

(defn redo-log [in-file-name out-file-name & {:keys [ini end with-wait rate] 
                                              :or {ini 0 
                                                   end 999999999
                                                   with-wait true
                                                   rate 1}}]
  (with-open [in (io/reader in-file-name)
              out (io/writer out-file-name)]
    (let [in-lines (->> 
                     (line-seq in)
                     (keep-indexed 
                       (fn [index line] 
                         (when (<= ini index end)
                           (println line)
                           line))))]
      (letfn [(logger [last-date line]
                (if-let [[line hora milli thread msg] (re-find LOG-LINE-TEMPLATE line)]
                  (let [[last-date wait] (calc-wait last-date hora milli)]
                    (if with-wait (Thread/sleep (/ wait rate)))
                    (doto out (.write line) .newLine .flush)
                    last-date)
                  last-date))]
        (reduce logger nil in-lines)))))

(defn send->arp [strm evt]
  (try
    (.println strm (pr-str evt))
    (catch Exception e
      (.printStackTrace e))))
              
(defn redo-log->arp [host-name service in-file-name
                     & {:keys [ini end with-wait rate arp-config] 
                        :or {ini 0 
                             end 9999999999
                             with-wait true
                             rate 1
                             arp-config {:host "localhost" :port 7777}}}]
  (with-open [in (io/reader in-file-name)
              skt (Socket. (:host arp-config) (:port arp-config))]
    (let [out (PrintStream. (BufferedOutputStream. (.getOutputStream skt)))
          in-lines (->> 
                     (line-seq in)
                     (keep-indexed 
                       (fn [index line] 
                         (if (<= ini index end) line))))]
      (letfn [(logger [last-date line]
                (println ">>>" line)
                (if-let [[line hora milli thread msg] (re-find LOG-LINE-TEMPLATE line)]
                  (let [[last-date wait] (calc-wait last-date hora milli)
                        msg-info (parse-cathel-msg msg)]
                    (if with-wait (Thread/sleep (/ wait rate)))
                    (send->arp out (merge
                                     {:host host-name
                                      :service service
                                      :state "ok"
                                      :description msg
                                      :metric 1 
                                      :ts (.getTime last-date)
                                      :thread thread}
                                     msg-info))
                    last-date)
                  last-date))]
        (reduce logger nil in-lines))))) 
         

(defn do-it []
  (redo-log
    "./server.log"
    "./out.log" 
    :ini 694000
    :rate 4))

(defn do2 [n]
  (redo-log->arp "ivx" "tx5" 
    "./big.log"
    :ini 0
    :end (+ n 0)
    :with-wait false
    :rate 1))




