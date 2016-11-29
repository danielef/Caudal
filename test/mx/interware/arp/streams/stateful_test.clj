(ns mx.interware.arp.streams.stateful-test
  (:require [clojure.test :refer [deftest is]]
            [mx.interware.arp.streams.common :as common]
            [mx.interware.arp.streams.stateful :as stateful]
            [mx.interware.arp.streams.stateless :as stateless]
            [mx.interware.arp.core.state :as state]
            [mx.interware.arp.core.atom-state :as atom-state]))

(deftest mixer-test-1
  (let [test-result (atom nil)
        now (System/currentTimeMillis)
        test-data [{:log-type "S" :mode 0 :ts now :id 1 :tx "transaccionPrueba"}
                   {:log-type "O" :mode 0 :ts now :id 2 :tx "transaccionPrueba"}
                   {:log-type "M" :mode 0 :ts now :id 3 :tx "transaccionPrueba"}
                   {:log-type "T" :mode 0 :ts now :id 4 :tx "transaccionPrueba"}
                   {:log-type "T" :mode 1 :ts now :id 5 :tx "transaccionPrueba"}
                   {:log-type "O" :mode 1 :ts now :id 6 :tx "transaccionPrueba"}
                   {:log-type "S" :mode 1 :ts now :id 7 :tx "transaccionPrueba"}]
        priority-map {["S" 0] 1
                      ["O" 0] 2
                      ["M" 0] 3
                      ["T" 0] 4
                      ["T" 1] 5
                      ["O" 1] 6
                      ["S" 1] 7}
        priority-key (juxt :log-type :mode)
        priority-fn (fn [e]
                      (if-let [priority (priority-map (priority-key e))]
                        priority
                        (throw (Exception. (str "INVALID LOG-TYPE AND MODE COMBINATION:" (priority-key e))))))
        children-fn (common/defstream [events]
                      (reset! test-result (into [] (map #(dissoc % :arp/latency) events)))
                      (locking java.lang.Object
                        (.notify java.lang.Object)))              
        streams (stateless/by [:tx]
                  (stateful/mixer :mixer-mem 400 :ts priority-fn children-fn))
        state (agent {})
        sink (common/create-sink state streams)]
    (doseq [event (shuffle test-data)]
      (if (vector? event)
        (doseq [e event]
          (sink e))
        (do
          (println :enviando event)
          (sink event))))
    (locking java.lang.Object 
      (.wait java.lang.Object 420))
    (is (= @test-result test-data))))
      
(deftest mixer-test-2
  (let [test-result (atom nil)
        now (System/currentTimeMillis)
        test-data [{:log-type "S" :mode 0 :ts now :id 1 :tx "transaccionPrueba"}
                   {:log-type "O" :mode 0 :ts now :id 2 :tx "transaccionPrueba"}
                   {:log-type "M" :mode 0 :ts now :id 3 :tx "transaccionPrueba"}
                   {:log-type "T" :mode 0 :ts now :id 4 :tx "transaccionPrueba"}
                   {:log-type "T" :mode 0 :ts now :id 5 :tx "transaccionPrueba"}
                   {:log-type "T" :mode 1 :ts now :id 6 :tx "transaccionPrueba"}
                   {:log-type "O" :mode 1 :ts now :id 7 :tx "transaccionPrueba"}
                   {:log-type "S" :mode 1 :ts now :id 8 :tx "transaccionPrueba"}]
        random-data [{:log-type "S" :mode 1 :ts now :id 8 :tx "transaccionPrueba"}
                     {:log-type "T" :mode 1 :ts now :id 6 :tx "transaccionPrueba"}
                     {:log-type "O" :mode 0 :ts now :id 2 :tx "transaccionPrueba"}
                     {:log-type "T" :mode 0 :ts now :id 4 :tx "transaccionPrueba"}
                     {:log-type "S" :mode 0 :ts now :id 1 :tx "transaccionPrueba"}
                     {:log-type "M" :mode 0 :ts now :id 3 :tx "transaccionPrueba"}
                     {:log-type "T" :mode 0 :ts now :id 5 :tx "transaccionPrueba"}
                     {:log-type "O" :mode 1 :ts now :id 7 :tx "transaccionPrueba"}]
                     
                     
        priority-map {["S" 0] 1
                      ["O" 0] 2
                      ["M" 0] 3
                      ["T" 0] 4
                      ["T" 1] 5
                      ["O" 1] 6
                      ["S" 1] 7}
        priority-key (juxt :log-type :mode)
        priority-fn (fn [e]
                      (if-let [priority (priority-map (priority-key e))]
                        priority
                        (throw (Exception. (str "INVALID LOG-TYPE AND MODE COMBINATION:" (priority-key e))))))
        children-fn (common/defstream [state events]
                      (reset! test-result (into [] (map #(dissoc % :arp/latency) events)))
                      (clojure.pprint/pprint state)
                      (println "************** state1 *************")
                      (doseq [quatro (get-in state [[:mixer-mem] :buffer])]
                        (println quatro))
                      (println "************** state2 *************")
                      (doseq [e events]
                        (println e))
                      (println (pr-str @test-result))
                      (println (pr-str test-data))
                      (println (= @test-result test-data))
                      (locking java.lang.Object
                        (.notify java.lang.Object)))
                                    
        streams (stateful/mixer :mixer-mem 500 :ts priority-fn children-fn)
        state (agent {})
        sink (common/create-sink state streams)]
    (doseq [event random-data]
      (if (vector? event)
        (doseq [e event]
          (sink e))
        (do
          (println :enviando event)
          (sink event))))
    (locking java.lang.Object 
      (.wait java.lang.Object 650))
    (Thread/sleep 1500)
    (println "************** state2 *************")
    (doseq [quatro (get-in [[:mixer-mem] :buffer] @state)]
      (println quatro))
    (is (= @test-result test-data))))
        
(deftest by-counter-1
  (let [expected {[:counter1 "tx1"] {:n 11, :ttl -1},
                  [:counter1 "tx2"] {:n 15, :ttl -1},
                  [:counter2 "tx2" "h1"] {:n 7, :ttl -1},
                  [:counter3 "h1"] {:n 12, :ttl -1},
                  [:counter2 "tx2" "h2"] {:n 8, :ttl -1},
                  [:counter3 "h2"] {:n 14, :ttl -1},
                  [:counter2 "tx1" "h2"] {:n 6, :ttl -1},
                  [:counter2 "tx1" "h1"] {:n 5, :ttl -1}}
        events (into [] (shuffle 
                          (flatten 
                            (conj 
                              (repeat 5 [{:tx "tx1" :host "h1"}]) 
                              (repeat 6 [{:tx "tx1" :host "h2"}])
                              (repeat 7 [{:tx "tx2" :host "h1"}])
                              (repeat 8 [{:tx "tx2" :host "h2"}])))))
        streams (stateless/default :ttl -1
                  (stateless/by [:tx]
                    (stateful/counter :counter1 :metric
                      (stateless/by [:host]
                        (stateful/counter :counter2 :metric))))
                  (stateless/by [:host]
                    (stateful/counter :counter3 :metric)))
                      
        state (agent {})
        sink (common/create-sink state streams)]
    (doseq [event events]
      (sink event))
    (println :to-sleep)
    (Thread/sleep 3000)
    (println :back)
    (println "@state : " (dissoc @state :arp/agent))
    (println "as-map:" (common/arp-state-as-map state))
    (let [result (into {} (map (fn [[k v]] [k (dissoc v :touched)]) (common/arp-state-as-map state)))]
      (clojure.pprint/pprint result)
      (clojure.pprint/pprint expected)
      (println (= result expected))
      (is (= result expected)))))
      
(deftest using-paths-1
  (let [expected {"/default/by(tx2)/counter" {:n 15, :ttl -1},
                  "/default/by(tx2)/counter/by(h1)/counter" {:n 7, :ttl -1},
                  "/default/by(h1)/counter" {:n 12, :ttl -1},
                  "/default/by(tx1)/counter" {:n 11, :ttl -1},
                  "/default/by(tx1)/counter/by(h2)/counter" {:n 6, :ttl -1},
                  "/default/by(h2)/counter" {:n 14, :ttl -1},
                  "/default/by(tx1)/counter/by(h1)/counter" {:n 5, :ttl -1},
                  "/default/by(tx2)/counter/by(h2)/counter" {:n 8, :ttl -1},
                  "/default/by(tx2)/counter/by/join/counter" {:n 15, :ttl -1} 
                  "/default/by(tx1)/counter/by/join/counter" {:n 11, :ttl -1}}
        events (into [] (shuffle 
                          (flatten 
                            (conj 
                              (repeat 5 [{:tx "tx1" :host "h1"}]) 
                              (repeat 6 [{:tx "tx1" :host "h2"}])
                              (repeat 7 [{:tx "tx2" :host "h1"}])
                              (repeat 8 [{:tx "tx2" :host "h2"}])))))
        streams (common/using-path 
                  [mx.interware.arp.streams.stateless]
                  mx.interware.arp.streams.stateful
                  (stateless/default :ttl -1
                    (stateless/by [:tx]
                      (stateful/counter :metric
                        (stateless/by [:host]
                          (stateful/counter :metric)
                          (stateless/join
                            (stateful/counter :contador)))))
                    (stateless/by [:host]
                      (stateful/counter :metric))))
        state (agent {})
        sink (common/create-sink state streams)]
    (doseq [event events]
      (sink event))
    (Thread/sleep 1000)
    (let [result (into {} (map (fn [[k v]] [k (dissoc v :touched)]) (common/arp-state-as-map state)))]
      (println "RESULT:")
      (clojure.pprint/pprint result)
      (println "EXPECTED:")
      (clojure.pprint/pprint expected)
      (println "ARE EQUAL?:" (= result expected))
      (is (= result expected)))))    

(deftest history-load-1
  (let [result (atom nil)
        events [{:arp/cmd :load-history
                 :path "config/stats-test"
                 :now-millis 604800000
                 :date-fmt "yyyyMM_ww" 
                 :key-name "history"}
                {:tx "getInqCustCardInfo" :cosa "buena"}]
        expected (merge
                   (second events)
                   (dissoc
                     (read-string (slurp "config/stats-test/201611_46-history-getInqCustCardInfo.edn"))
                     :touched)) 
        streams (stateless/default :ttl -1
                  (common/defstream [e] (prn e))
                  (stateless/decorate (fn [e] [:history (:tx e)])
                    (common/defstream [e] 
                      (println "despues de decorate:" e))
                    (fn cosita[by-path state e]
                      (reset! result e)
                      (locking result
                        (println "******* notify ****")
                        (.notify result))
                      state)))
        state (agent {})
        sink  (common/create-sink state streams)]
    (locking result
      (doseq [event events]
        (sink event))
      (.wait result 5000))
    (println "*** SALIO:" @result)
    (is (= (dissoc @result :touched :arp/latency) 
           expected)))) 

(deftest percentiles-1
  (let [cuantos 10000
        rango-random 1000
        percents [0.25 0.5 0.75 0.95]
        metrics (vec (for [x (range 0 cuantos)] (rand-int rango-random)))
        expected (let [sorted (sort metrics)
                       idx-fn (fn [percent]
                                (min (dec cuantos) (Math/floor (* percent cuantos))))]
                   (into {} (map (fn [p]
                                   [p (nth sorted (idx-fn p))]) percents)))
                       
        events (into [] (shuffle 
                          (map-indexed (fn [idx rnd]
                                         {:tx "tx1" :host "h1" :delta rnd :idx idx}) metrics)))
        streams (stateless/default :ttl -1
                  (stateless/by [:tx]
                    (stateful/counter :counter1 :metric
                      (stateless/->INFO :all))
                    (stateful/batch :batch-1 10000 2000
                      (stateless/percentiles :delta :percentiles percents 
                        (common/defstream [e]
                          (println :los-percentiles (pr-str e)))
                        (stateless/store! (fn [e] :percentiles))))))  
        state (agent {})
        sink (common/create-sink state streams)]
    (doseq [event events]
      (sink event))
    (println :to-sleep)
    (Thread/sleep 5000)
    (println :back)
    ;(println "as-map3:")
    ;(clojure.pprint/pprint @state)
    (println "as-map2:" (filter (fn [[k v]] 
                                  (= :percentiles k)) 
                          (common/arp-state-as-map state)))
    (let [result (-> @state :percentiles :percentiles)]
      (clojure.pprint/pprint result)
      (clojure.pprint/pprint expected)
      (println (= result expected))
      (is (= result expected)))))

            
