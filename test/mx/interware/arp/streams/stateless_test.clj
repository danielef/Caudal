(ns mx.interware.arp.streams.stateless-test
  (:require [mx.interware.arp.streams.stateless :as stateless]
            [clojure.test :refer [deftest is with-test]]
            [mx.interware.arp.streams.stateful :as stateful]
            [mx.interware.arp.streams.common :as common]
            [mx.interware.arp.core.state :as state]
            [mx.interware.arp.core.atom-state :as atom-state]
            [mx.interware.arp.streams.common :as common]))

(deftest default-test-1
  (let [input-test {:tx "transaction"}
        test {:param :ttl :value -1}
        expected-result-1 {:tx "transaction" :ttl -1}
        result1 (atom {})]
    (def streams (stateless/default (:param test) (:value test) #(reset! result1 %)))
    (streams input-test)
    (is (= expected-result-1 @result1))))
