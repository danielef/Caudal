(defproject mx.interware/arp "1.1.0"
  :description "Scavenger"
  :url "http://arp.interware.mx/"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  
  :plugins      [[lein-libdir "0.1.1"]
                 [codox "0.8.10"]]
  
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/core.async "0.2.391"]
                 [org.clojure/data.codec "0.1.0"]
                 [org.clojure/data.json "0.2.6"]
                 [org.clojure/tools.logging "0.3.1"]
                 [clj-http "2.3.0"]
                 ;[clj-time "0.12.2"]
                 [clojurewerkz/elastisch "2.2.2"]
                 [org.apache.mina/mina-core "2.0.15"]
                 [org.slf4j/slf4j-log4j12 "1.7.5"]
                 [commons-io/commons-io "2.5"]
                 ;[net.mikera/core.matrix "0.56.0"]

                 [bidi "2.0.14"]
                 [com.domkm/silk "0.1.2"]
                 [org.apache.kafka/kafka-clients "0.10.1.0"]

                 [aleph "0.4.1"]
                 [gloss "0.2.5"]
                 [org.immutant/immutant "2.1.5"]
                 [avout "0.5.3"]
                 [org.syslog4j/syslog4j "0.9.46"]
                 [com.draines/postal "2.0.2"]
                 [hiccup "1.0.5"]]
  
  :main mx.interware.arp.core.starter

  :repl-options {:prompt (fn [ns] (str "<" ns "> "))
                 :welcome (println "Welcome to the magical world of the repl!")
                 :init-ns mx.interware.arp.core.starter}

  :source-paths ["src"]
  :test-paths ["test"]
  
  :codox {:defaults {:doc/format :markdown}}
  :aot [mx.interware.arp.core.global
        mx.interware.arp.core.main
        mx.interware.arp.core.starter
        mx.interware.arp.core.state
        mx.interware.arp.streams.common
        mx.interware.arp.streams.stateless
        mx.interware.arp.streams.stateful
        mx.interware.arp.io.client
        mx.interware.arp.io.elastic
        mx.interware.arp.io.server
        mx.interware.arp.io.tcp-server
        mx.interware.arp.io.tailer-server
        mx.interware.arp.io.log4j-server
        mx.interware.arp.io.syslog-server
        mx.interware.arp.core.scheduler-server
        
        ;mx.interware.arp.util.crypt-util
        mx.interware.arp.util.date-util
        mx.interware.arp.util.id-util
        mx.interware.arp.util.rest-util])
  
  
      

  
