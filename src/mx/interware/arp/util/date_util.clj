(ns mx.interware.arp.util.date-util
  (:import [java.util Date]
           [java.text SimpleDateFormat]))

(def ymd-format "yyyy-MM-dd")
(def ymdhms-format "yyyy-MM-dd HH:mm:ss.SSS")
(def zulu-format "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")

(def MILLIS-IN-DAY (* 1000 3600 24))

(def TZ-OFFSET (* -1 6 1000 3600))

(defn compute-rate-bucket [millis bucket-size]
  (let [adjusted (+ millis TZ-OFFSET)
        day (int (/ adjusted MILLIS-IN-DAY))
        in-minutes (int (/ adjusted 1000 60))
        minute (int (/ (mod in-minutes 60) bucket-size))
        hour (mod (int (/ in-minutes 60)) 24)]
    [day hour minute]))    


(defn parse-date [date-fmt date-str]
  (let [sdf (SimpleDateFormat. date-fmt)
        len (count date-str)
        date-fmt-len (count date-fmt)
        date-adjusted (if (< len date-fmt-len)
                        (apply str date-str (repeat (- date-fmt-len len) "0"))
                        date-str)]
    (.parse sdf date-adjusted)))

(defn format-date [date-fmt date]
  (let [sdf (SimpleDateFormat. date-fmt)]
    (.format sdf date)))

(defn current-ymd-date-str []
  (.format (SimpleDateFormat. ymd-format) (Date.)))

(defn current-ymdhms-time-str []
  (.format (SimpleDateFormat. ymdhms-format) (Date.)))

(defn current-zulu-time-str []
  (.format (SimpleDateFormat. zulu-format) (Date.)))

(def time-metric-map
  {:second 1000
   :seconds 1000
   :minute 60000
   :minutes 60000
   :hour 3600000
   :hours 3600000
   :day   86400000
   :days  86400000
   :week  604800000
   :weeks 604800000})

(defn cycle->millis [cycle-expr]
  (cond 
    (number? cycle-expr)
    cycle-expr
    
    (vector? cycle-expr)
    (let [[n metric] cycle-expr
          millis-factor (metric time-metric-map)]
      (* n millis-factor))))
         
   
