(ns mx.interware.arp.core.email-test
  (:require [clojure.test :refer [deftest is]]
             [hiccup.core :as hiccup :refer [html]]
            [mx.interware.arp.core.email :as email]))

(deftest test-make-header
  (is (= (email/make-header "bonjour!") 
         [:div {:class "header", :style "margin:5px 10px; padding:0"} 
          [:b "bonjour!"]])))

(deftest test-make-footer
  (is (= (email/make-footer)
         [:div {:class "footer", :style "width: 550px; text-align: right;"} 
          [:p {:style "color: #8899a6"} "Powered by Arquimedes &copy;"]])))

(deftest test-make-table
  (let [event {:host "semiorka" :app "tsys-op-wallet" :metric 11235.813}]
    (is (= (hiccup/html (email/make-table-template event))
           (hiccup/html [:table {:style "margin:5px 10px; border:2px solid #f5f8fa; padding:0" 
                    :cellpadding "5", :cellspacing "0", :width "550px"} 
            [:tbody 
             [:tr {:style "background-color:#f5f8fa"} [:th {:width "20%", :align "left"} "host"] [:td {:width "80%", :align "left"} "semiorka"]] 
             [:tr {:style "background-color:#ffffff"} [:th {:width "20%", :align "left"} "app"] [:td {:width "80%", :align "left"} "tsys-op-wallet"]] 
             [:tr {:style "background-color:#f5f8fa"} [:th {:width "20%", :align "left"} "metric"] [:td {:width "80%", :align "left"} 11235.813]]]])))))

(deftest test-event->html
  (let [event {:mode 0 :ts (System/currentTimeMillis) :id 1 :tx "tx-test"}
        templ [:table {:style "margin:5px 10px; border:2px solid #f5f8fa; padding:0"
                       :cellpadding "5" :cellspacing "0" :width "550px"} 
               [:tbody (map-indexed (fn [i key] 
                                      (let [style (if (even? i) 
                                                    "background-color:#f5f8fa"
                                                    "background-color:#ffffff")]
                                        [:tr {:style style} 
                                         [:th {:width "20%" :align "left"} (name key)]
                                         [:td {:width "80%" :align "left"} key]])) 
                                    (keys event))]]]
    (is (= (email/event->html event)
           (email/event->html event templ)))))

;; TODO: Integration
;(deftest test-email-event)

;; TODO: Integration
;(deftest test-mailer)

