(ns mx.interware.arp.core.main
  (:require
    [clojure.pprint :as pp]
    [clojure.string :as str])              
  (:import 
    (java.net 
      InetAddress URL)))
           
(defn state-reduce [state e children]
  (loop [[child & children] children
         state state]
    (if child
      (recur children (if-let [n-state (child state e)]
                        n-state
                        state))
      state)))

(defn counter [path metric-kwd & children]
  (fn [state evt]
    (println :state state evt)
    (let [cntr (get-in state path 0)
          metric (metric-kwd evt)
          new-c (+ cntr metric)
          new-state (assoc-in state path new-c)]
      (println :new-state new-state)
      (state-reduce
        new-state
        (assoc evt metric-kwd new-c)
        children))))

(defn pprint [path & children]
  (fn [state e]
    (println "STATE:")
    (clojure.pprint/pprint state)
    (println "e:" (pr-str e))
    (state-reduce state e children)))

(defn default [path kwd val & children]
  (fn [state e]
    (state-reduce
      state
      (assoc e kwd (kwd e val))
      children)))

(defn with [path kwd val & children]
  (fn [state e]
    (state-reduce
      state
      (assoc e kwd val)
      children)))

(def A (atom {}))
@A

(comment origin
  (default :default :ttl 60
    (counter :s-met :metric
      (pprint :pprint))
    (with :uno :x 1 [/ .. .. 3 :metric]
      (counter :e-cnt :x
        (pprint :final)))))

(def c 
  (default [:default] :ttl 60
    (counter [:default :s-met] :metric
      (pprint [:default :s-met :pprint]))
    (with [:default :uno] :x 1
      (counter [:default :uno :e-cnt] :x
        (pprint [:default :uno :e-cnt :final])))))
(swap! A c {:a 1 :metric 5})
(swap! A c {:a 1 :metric 5})
(swap! A c {:a 1 :metric 5})
(swap! A c {:a 1 :metric 5})
(swap! A c {:a 1 :metric 3})

(swap! A c {:a 1 :metric 3})

(defprotocol Countable
  (cnt [x]))
(class "class")
(extend-type java.lang.String
  Countable
  (cnt [this] (count this)))



