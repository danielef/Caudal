(ns mx.interware.arp.util.rest-util
  (:require [clojure.tools.logging :as log]
            [clj-http.client :as client])
  (:import [java.util Date]
           [java.text SimpleDateFormat]))

(defn put-event [idx tx ts mean]
  (let [body (str "{\"tx\": \"" tx "\", \"ts\": \"" ts "\", \"mean\": \"" mean "\"}")
        url (str "http://localhost:9200/log/" idx)]
    (log/info "Putting message on elasticsearch [" url "] : " body " ...")
    (client/put url
                {:form-params      body
                 :content-type     :json
                 :throw-exceptions false
                 :as               :json})))

