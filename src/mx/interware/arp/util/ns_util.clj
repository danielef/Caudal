(ns mx.interware.arp.util.ns-util
  (:require [clojure.string :refer [split]]))

(defn resolve&get-fn [fully-qualified-fn]
  (let [fn-ns (symbol (namespace fully-qualified-fn))
        _     (require fn-ns)]
    (resolve fully-qualified-fn)))    

(defn require-name-spaces [name-spaces]
  (doseq [ns-str name-spaces]
    (let [ns-symbol (symbol ns-str)]
      (println "requiring store implementation:" ns-symbol "...")
      (require ns-symbol))))
