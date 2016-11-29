(ns mx.interware.arp.io.syslog-server-test
  (:use mx.interware.arp.io.syslog-server
        clojure.test)
  (:require [clojure.tools.logging :as log])
  (:import 
   (org.productivity.java.syslog4j.impl.message.structured
    StructuredSyslogMessage)))

(defn lstr [& strings] (clojure.string/join "" strings))

(def inet-addr (java.net.InetAddress/getByName "127.0.0.1"))

;; Structured data empty
(def data-structured-instance-0
  (lstr 
   "<34>1 2003-10-11T22:14:15.003Z mymachine.example.com su - ID47"
   " - BOM'su root' failed for lonvick on /dev/pts/8"))

;; Structured data
(def data-structured-instance-1 
  (lstr 
   "<134>1 2012-07-25T21:32:08.234Z some-server.some.domain noprog qtp"
   "583592918-80437 95d42b22c48e4eadb59e61a182c102d4 [l@2][a@3 a=\"b\\\"c\"]"))

;; Non structured data without timezone
(def data-non-structured-instance-0
  (lstr 
   "<34>Oct 11 22:14:15 mymachine su: 'su root' failed for lonvick on "
   "/dev/pts/8"))

;; Non structured data
(def data-non-structured-instance-1
  (lstr 
   "<165>Aug 24 05:34:00 CST 1987 mymachine myproc[10]: %% It's "
   "time to make the do-nuts.  %%  Ingredients: Mix=OK, Jelly=OK # "
   "Devices: Mixer=OK, Jelly_Injector=OK, Frier=OK # Transport: "
   "Conveyer1=OK, Conveyer2=OK # %%"))

(deftest valid-structured-data-0
  (is (structured-data? data-structured-instance-0)))

(deftest valid-structured-data-1
  (is (structured-data? data-structured-instance-1)))

(deftest valid-structured-data-2
  (is (not (structured-data? data-non-structured-instance-0))))

(deftest valid-structured-data-3
  (is (not (structured-data? data-non-structured-instance-1))))

(deftest structured-data-empty
  (let [event (data->event data-structured-instance-0 inet-addr nil)]
    (is (= event 
           {:facility 4 
            :length 110 
            :level 2 
            :host "mymachine.example.com" 
            :message "ID47 - BOM'su root' failed for lonvick on /dev/pts/8"
            :timestamp 1065910455003 
            :messageId nil}))))

(deftest structured-data-filled 
  (let [event (data->event data-structured-instance-1 inet-addr nil)]
    (is (= event 
           {:facility 16 
            :length 134 
            :level 6
            :host "some-server.some.domain"
            :message ""
            :timestamp 1343251928234
            :messageId "95d42b22c48e4eadb59e61a182c102d4"
            "a@3" {"a" "b\\\"c"}
            "l@2" {}}))))

(deftest non-structured-without-timezone 
  (let [event (data->event data-non-structured-instance-0 inet-addr nil)]
    (is (= event
           {:facility 4
            :length 76
            :level 2
            :host "localhost"
            :message "mymachine su: 'su root' failed for lonvick on /dev/pts/8"
            :timestamp 1476242055000}))))

(deftest non-structured-with-timezone 
  (let [event (data->event data-non-structured-instance-1 inet-addr nil)]
    (is (= event 
           {:facility 20
            :length 214, 
            :level 5, 
            :host "localhost", 
            :message (lstr "CST 1987 mymachine myproc[10]: %% It's time to make"
                           " the do-nuts.  %%  Ingredients: Mix=OK, Jelly=OK # "
                           "Devices: Mixer=OK, Jelly_Injector=OK, Frier=OK # Tr"
                           "ansport: Conveyer1=OK, Conveyer2=OK # %%")
            :timestamp 1472034840000}))))
