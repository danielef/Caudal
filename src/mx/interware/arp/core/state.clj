(ns mx.interware.arp.core.state
  (:require [clojure.pprint :as pp]
            [clojure.string :as str]
            [clojure.tools.logging :as log]))


(defprotocol ARPstore
  (as-map [this])
  (store! [this k v])
  (lookup [this k] [this k default])
  (remove! [this k])
  (clear-all! [this])
  (clear-keys! [this key-filter])
  (update! 
    [this k fun] 
    [this k fun a1] 
    [this k fun a1 a2] 
    [this k fun a1 a2 a3] 
    [this k fun a1 a2 a3 a4]
    [this k fun a1 a2 a3 a4 a5]
    [this k fun a1 a2 a3 a4 a5 a6]
    [this k fun a1 a2 a3 a4 a5 a6 a7]
    [this k fun a1 a2 a3 a4 a5 a6 a7 a8]
    [this k fun a1 a2 a3 a4 a5 a6 a7 a8 a9]
    [this k fun a1 a2 a3 a4 a5 a6 a7 a8 a9 a10]))
  
(defn selector [type-name & params]
  type-name)

(defmulti create-store selector) 
