(ns mx.interware.arp.io.tcp-server
  (:require [clojure.tools.logging :as log]
            [mx.interware.arp.streams.common :refer [start-listener]])
  (:import (java.net InetSocketAddress)
           (java.nio.charset Charset)
           (org.apache.log4j PropertyConfigurator)
           (org.apache.mina.core.session IdleStatus)
           (org.apache.mina.filter.codec ProtocolCodecFilter)
           (org.apache.mina.filter.codec.textline TextLineCodecFactory)
           (org.apache.mina.filter.logging LoggingFilter)
           (org.apache.mina.transport.socket.nio NioSocketAcceptor)
           (org.apache.mina.core.session IoSession IdleStatus)))           

(defn read-event [str]
  (try
    (read-string str)
    (catch Exception e
      nil)))

(defn create-handler [sink]
  (let [handler (reify org.apache.mina.core.service.IoHandler
                  (^void exceptionCaught [this ^IoSession session ^Throwable cause]
                    (.printStackTrace cause))
                  (^void inputClosed [this ^IoSession session])
                  (^void messageReceived [this ^IoSession session ^Object message]
                    (let [message-str (.toString message)]
                       (when-let [event (read-event message-str)]
                         (if (= "EOT" message-str)
                           (.closeOnFlush session)
                           (do
                             (if (vector? event)
                               (doseq [e event]
                                 (sink e))
                               (sink event)))))))
                  (^void messageSent [this ^IoSession session ^Object message])
                  (^void sessionClosed [this ^IoSession session])
                  (^void sessionCreated [this ^IoSession session])
                  (^void sessionIdle [this ^IoSession session ^IdleStatus status]
                    (println "IDLE " (.getIdleCount session status)))
                  (^void sessionOpened [this ^IoSession session]))]
    handler))

(defn start-server [port idle-period sink]
  (try 
    (let [buffer-size    4096
          max-line-length (* 4 8192)
          acceptor       (new NioSocketAcceptor)
          codec-filter   (new ProtocolCodecFilter 
                           (doto
                             (new TextLineCodecFactory (Charset/forName "UTF-8"))
                             (.setDecoderMaxLineLength max-line-length)
                             (.setEncoderMaxLineLength max-line-length)))
          filter-chain   (.getFilterChain acceptor)
          session-config (.getSessionConfig acceptor)
          handler        (create-handler sink)
          socket-address (new InetSocketAddress port)]
      (log/info "Starting server on port : " port " ...")
      (.addShutdownHook (Runtime/getRuntime) (Thread. (fn [] 
                                                        (println "Shutting down mina...")
                                                        (.dispose acceptor true))))
      (.addLast filter-chain "logger" (new LoggingFilter))
      (.addLast filter-chain "codec" codec-filter)
      (.setHandler acceptor handler)
      (.setReadBufferSize session-config buffer-size)
      (.setIdleTime session-config (IdleStatus/BOTH_IDLE) idle-period)
      (.bind acceptor socket-address))
    (catch Exception e
      (.printStackTrace e))))

(defmethod start-listener 'mx.interware.arp.io.tcp-server
  [sink config]
  (let [{:keys [port idle-period]} (get-in config [:parameters])]
    (start-server port idle-period sink)))


