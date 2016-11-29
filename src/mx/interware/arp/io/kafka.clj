(ns mx.interware.arp.io.kafka
  (:require [clojure.tools.logging :as log]
            [clojure.core.async :refer [chan go go-loop timeout <! >! <!! >!!]]
            [mx.interware.arp.streams.common :refer [propagate start-listener]])
  (:import (java.util Date Properties)
           (org.apache.log4j PropertyConfigurator)
           (org.apache.kafka.clients.producer KafkaProducer Producer ProducerRecord)
           (org.apache.kafka.clients.consumer KafkaConsumer ConsumerRecord ConsumerRecords)
           (org.apache.commons.io FileUtils)))

(PropertyConfigurator/configure "log4j.properties")

(defn map-to-properties [m]
  (let [properties (new Properties)]
    (doall (map (fn [[k v]] (.put properties (name k) (str v))) (seq m)))
    properties))

(defn create-producer [parameters]
  (new KafkaProducer (map-to-properties parameters)))

(defn create-consumer [parameters]
  (new KafkaConsumer (map-to-properties parameters)))

(defn send-event [producer topic-name event]
  (log/info (str "Sending event : " (pr-str event) " ..."))
  (.send producer (new ProducerRecord topic-name (pr-str event))))

(defn subscribe [consumer topic-name]
  (let [topics (list topic-name)]
    (.subscribe consumer topics)
    (let [channel (chan 1)]
      (future
        (while true
          (let [records-iterator (.poll consumer 100)
                records          (doall (iterator-seq (.iterator records-iterator)))]
            (doseq [record records]
              (do
                (println "Record : " record)
                (println "Received event : " (.value record))
                (>!! channel (read-string (.value record))))))))
      channel)))

(defn start-processing [channel sink]
  (go-loop []
    (let [event (<! channel)]
      (println "Processed event : " (pr-str event))
      (sink event)
      (recur))))

(def producer-parameters {:bootstrap.servers "localhost:9092"
                          :acks              "all"
                          :retries           0
                          :batch.size        16384
                          :linger.ms         1
                          :buffer.memory     33554432
                          :key.serializer    "org.apache.kafka.common.serialization.StringSerializer"
                          :value.serializer  "org.apache.kafka.common.serialization.StringSerializer"})


(def consumer-parameters {:bootstrap.servers       "localhost:9092"
                          :group.id                "test"
                          :enable.auto.commit      true
                          :auto.commit.interval.ms 1000
                          :session.timeout.ms      30000
                          :key.deserializer        "org.apache.kafka.common.serialization.StringDeserializer"
                          :value.deserializer      "org.apache.kafka.common.serialization.StringDeserializer"})

(defn kafka-send [topic-name parameters & children]
  (let [producer (create-producer parameters)]
    (fn stream [event]
      (send-event producer topic-name event)
      (propagate event children))))

(comment
  (def topic-name "test")
  (def event {:a 1 :b "dos" :c 3.0 :d (new java.util.Date)})
  (def consumer (create-consumer consumer-parameters))
  (def producer (create-producer producer-parameters))

  (def channel (subscribe consumer topic-name))
  (start-processing channel)

  (send-event producer topic-name event)
  )

(defmethod start-listener :kafka-server [sink states config]
  (let [{:keys [topic-name consumer-parameters]} (get-in config [:parameters])
        consumer (create-consumer consumer-parameters)
        channel  (subscribe consumer topic-name)]
    (start-processing channel sink)))
