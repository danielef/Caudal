(ns mx.interware.arp.io.tailer-server
  (:require
    [clojure.pprint :as pp]        
    [clojure.java.io :as io]
    [clojure.tools.logging :as log]
    [clojure.string :as s]
    [clojure.core.async :refer [chan >!! alts! go-loop buffer]]
    [mx.interware.arp.streams.common :refer [start-listener]]
    [mx.interware.arp.util.ns-util :refer [resolve&get-fn require-name-spaces]])
  (:import
    (org.apache.commons.io.input Tailer TailerListener TailerListenerAdapter)))

(defn create-listener [line-channel]
  (let [listener (reify org.apache.commons.io.input.TailerListener
                   (^void handle [this ^String line] 
                     (>!! line-channel line))
                   (^void handle [this ^Exception ex] (do
                                                        (log/info "Handling exception ..." ex)
                                                        (.printStackTrace ex)))
                   (init [this tailer])
                   (fileNotFound [this])
                   (fileRotated [this]))]
    listener))

(defn initialize-tail [files delta from-end reopen buffer-size]
    (log/info "Tailing files : " files " ...")
    (doall (map
             (fn [file-name]
               (let [line-channel (chan)
                     listener (create-listener line-channel)
                     file (io/file file-name)
                     tailer (Tailer. file listener delta from-end reopen buffer-size)]
                 (future (.run tailer))
                 (log/info "Channel created for file " file-name " - > channel : " line-channel)
                 line-channel))
             files)))

(defn register-channels [channels parse-fn sink]
  (println "register-channels for tailer")
  (go-loop []
    (try
      (some-> channels alts! first parse-fn sink)
      (catch Exception e
        (.printStackTrace e)
        (System/exit 0)))
    (recur)))
      
  
  
(defmethod start-listener 'mx.interware.arp.io.tailer-server 
  [sink config]
  (let [{:keys [files delta from-end reopen buffer-size parser]} (get-in config [:parameters])
        log-channels     (initialize-tail files delta from-end reopen buffer-size)
        parse-fn         (resolve&get-fn parser)]
    (register-channels log-channels parse-fn sink)))
  
  
  
