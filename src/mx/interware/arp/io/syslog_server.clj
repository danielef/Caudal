(ns mx.interware.arp.io.syslog-server
  (:require [clojure.tools.logging :as log]
            [mx.interware.arp.streams.common :refer [start-listener]]
            [mx.interware.arp.util.ns-util :refer [resolve&get-fn require-name-spaces]])
  (:import 
   (java.net InetSocketAddress ServerSocket Socket SocketException)
   (java.io BufferedReader InputStreamReader)
   (org.productivity.java.syslog4j.server.impl.event SyslogServerEvent)
   (org.productivity.java.syslog4j.server.impl.event.structured 
    StructuredSyslogServerEvent)))

(defn structured-data? 
  "Return true if receive-data is structured. When receive-data is a structured 
  syslog message must be parsed with StructuredSyslogServerEvent, see:
  [https://tools.ietf.org/html/rfc5424#section-6.5] otherwise, with standard 
  SyslogServerEvent, see: [https://tools.ietf.org/html/rfc3164#section-5.4]
  - *receive-data* Syslog data to be parsed"
  [receive-data]
  (let [idx (.indexOf receive-data ">")]
    (and (not= idx -1) 
         (> (.length receive-data) (inc idx)) 
         (Character/isDigit (.charAt receive-data (inc idx))))))

(defn data->event 
  "Converts received-data to ARP Event
  - *receive-data* Syslog data to be parsed
  - *inaddr* InetAddress to build a ServerEvent
  - *message-parser* to parse message field"
  [receive-data inaddr message-parser]
  (try 
    (let [object      (if (structured-data? receive-data)
                        (StructuredSyslogServerEvent. receive-data inaddr)
                        (SyslogServerEvent. receive-data inaddr))
          data-map    (bean object)
          {:keys [date facility rawLength level host message]} data-map
          arp-event   (merge
                       {:facility  facility
                        :length    rawLength
                        :level     level
                        :host      host
                        :message   message
                        :timestamp (.getTime date)}
                       (when-let [st-msg (:structuredMessage data-map)]
                         (let [msg-m (bean st-msg)                               
                               {:keys [message messageId structuredData]} msg-m]
                           (merge {:message message
                                   :messageId messageId}
                                  (into {} structuredData))))
                       (when message-parser (message-parser message)))]
      arp-event)
    (catch Exception e
      (log/error "Error " e))))

(defn start-server 
  "Starts TCP Syslog Server
  - *port* Port to listen Syslog data
  - *message-parser* to parse message field
  - *sink* to pass ARP Events"
  [port message-parser sink]
  (let [sserv (ServerSocket. port)]
    (future
      (while (not (.isClosed sserv))
        (try          
          (let [socket (.accept sserv)
                inaddr (.getInetAddress socket)
                _      (log/info :accepting-syslog-connections :port port)
                br     (BufferedReader. (InputStreamReader. 
                                         (.getInputStream socket)))]            
            (loop [line (.readLine br)]
              (let [arp-event (data->event line inaddr message-parser)]
                (sink arp-event)
                (recur (.readLine br)))))
          (catch SocketException e
            (log/warn (.getMessage e)))
          (catch Exception e
            (.printStackTrace e)))))
    sserv))

(defmethod start-listener 'mx.interware.arp.io.syslog-server
  [sink config]
  (let [{:keys [port parser]} (get-in config [:parameters])
        parse-fn         (if parser (resolve&get-fn parser))]
    (start-server port parse-fn sink)))


