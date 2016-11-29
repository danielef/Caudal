(ns mx.interware.arp.core.parsers)

(defn re-parser [{:keys [re tags] :or {re #".*" tags [:text]}}]
  (fn [line]
    (when-let [groups (re-matches re line)]
      (reduce 
        (fn [result [tag val]]
          (if (and (not= :_ tag) val)
            (assoc result tag val)
            result))
        {}
        (map (fn [tag val]
               [tag val])
          tags groups)))))
