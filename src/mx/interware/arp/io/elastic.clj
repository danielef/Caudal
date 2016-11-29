(ns mx.interware.arp.io.elastic
  (:require [clojure.tools.logging :as log]
            [clojure.data.json :as json]
            [clj-http.client :as http-client]
            [mx.interware.arp.util.id-util :as id-util]
            [mx.interware.arp.streams.common :refer [propagate]]
            [clojurewerkz.elastisch.rest :as esr]
            [clojurewerkz.elastisch.rest.document :as doc]
            [clojurewerkz.elastisch.rest.bulk :as bulk]
            [slingshot.support :refer [stack-trace]]))


(comment defn put-document [index type id body]
  (let [url      (str "http://elasticsearch:9200/" index "/" type "/" id "?pretty=true")
        json-str (json/write-str body)]
    (http-client/put url
                     {:form-params      json-str
                      :content-type     :json
                      :throw-exceptions false
                      :as               :json})))
                      
(comment defn find-statistics [index event-type initial-date final-date]
  (let [field (if (= "tx" event-type) "tx-name" "operation")
        url   (str "http://elasticsearch:9200/" index "/_search?pretty=true")
        query {:query {:bool {:must [{:match {:event-type event-type}},
                                     {:range {:ref-time  { :gte  initial-date :lt final-date}}}]}},
               :aggregations {:langs {:terms {:field field,
                                              :min_doc_count 20},
                                      :aggregations {:grades_stats {:extended_stats {:field "metric"}}}}}}
        dlog  (clojure.pprint/pprint query)]
    (log/info "Findding statistics for index : " index ", event-type : " event-type ", initial-date : " initial-date ", final-date : " final-date)
    (http-client/post url
                      {:form-params      query
                       :content-type     :json
                       :throw-exceptions false
                       :as               :json})))

(defn elastic-store!
  "
  Streamer function used for storing event(s) into Elasticsearch database.
  > **Arguments:**
     *elsh-url*: Elasticsearch connection URL
     *index*: Elasticsearch index
     *type*: Elasticsearch document type
     *children*: Children streamer functions to be propagated
  "
  [elsh-url index type & children]
  (let [elsh-connection (esr/connect elsh-url)]
    (fn stream [state event-input]
      (let [_              (log/info (str "========= event-input : " event-input))
            event-list     (cond (map? event-input)  (list event-input)
                                 (coll? event-input) event-input
                                 :else               (list event-input))
            _              (log/info (str "========== event-list : " event-list))
            elastic-events (map (fn [{:keys [doc-type] :or {doc-type (or type "default")} :as e}]
                                  (-> e (assoc :created-on (System/currentTimeMillis) :_type doc-type) (dissoc :ttl)))
                                event-list)
            operations     (bulk/bulk-index elastic-events)]
        (bulk/bulk-with-index elsh-connection index operations {:refresh true})
        (propagate state event-input children)))))
