(ns mx.interware.arp.core.global)

(comment
  (ns mx.interware.arp.core.global
    (:require [clojure.tools.logging :as log]
              [mx.interware.arp.streams.common :refer [propagate]]
              [mx.interware.arp.io.elastic :as el-util])
    (:import  (java.util Date))))

(comment defn add-value [key value & children]
  (fn stream [event]
    (let [new-event (assoc event key value)]
      (propagate new-event children))))

(comment defn log-event [prefix]
  (fn stream [event]
    (log/info prefix event)))

(comment defn store-unique-event [index doc-type]
  (fn stream [event]
    (el-util/put-document index doc-type "1" event)))

(comment defn dummy [& children]
  (fn stream [event]
    (log/info (str "[" (Date.) "] Inside dummy stream with event : " event " ..."))
    (propagate event children)))

(comment defn threshold [value & children]
  (fn stream [event]
    (if (> (:metric event) value)
      (propagate event children))))

(comment defn is-older? [now delta evt]
  (let [result (< (* 1000 (+ (:time evt) delta)) now)]
    result))

(comment def tmp (atom []))

(comment defn do-filter-sort [{:keys [events] :as buf} delta key-fun]
  (when (seq events)
    (let [now (System/currentTimeMillis)
          older? (partial is-older? now delta)
          t0 (System/currentTimeMillis)
          sorted-events (sort-by key-fun events)
          n-events (count events)
          to-dump (->> sorted-events (take-while older?))
          events (->> sorted-events
                      (drop-while older?))
          t1 (System/currentTimeMillis)
          dummy (swap! tmp conj [n-events (- t1 t0)])]
      {:events (seq events)
       :to-dump (seq to-dump)})))


(comment defn filter-sort&dump [buf delta key-fun children]
  (when-let [to-dump (:to-dump (swap! buf do-filter-sort delta key-fun))]
    (doseq [e to-dump]
      (streams/call-rescue e children))))

(comment defn mixer
  [delta key-fun & children]
  (let [buf (atom {})]
    (time/every! delta (partial filter-sort&dump buf delta key-fun children))
    (fn stream [evt]
      (swap! buf update :events conj evt))))


(comment defn send-mail [to subject body]
  (let [host ""
        from ""
        password ""]
    mailer {:from from
            :host "smtp.gmail.com"
            :user "foo"
            :pass "bar"}))
