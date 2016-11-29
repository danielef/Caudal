(ns mx.interware.arp.util.arp-util
  (:require [clojure.tools.logging :as log]))

(defn default-arp-agent-validator [agent-value]
  (when-not (map? agent-value)
    (throw (java.lang.RuntimeException. (str "Value for ARP state must be a map, intended value: " (pr-str agent-value)))))
  true)

(defn default-arp-agent-error-handler [agent-value exception]
  (log/warn exception))

(defn create-arp-agent [& {:keys [initial-map validator error-handler] 
                           :or {initial-map {}
                                validator default-arp-agent-validator
                                error-handler default-arp-agent-error-handler}}]
  (agent initial-map 
    :validator validator
    :error-handler error-handler
    :error-mode :continue))
