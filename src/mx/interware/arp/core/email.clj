(ns mx.interware.arp.core.email
  (:require [clojure.string :as str :refer [join]]
            [clojure.walk :as wlk :refer [walk]]
            [hiccup.core :as hiccup :refer [html]]
            [postal.core :as postal :refer [send-message]]))

(defn make-header
  "Creates a default header with a simple title
   
  * *title* replaced in mail header"
  [title]
  [:div {:class "header" :style "margin:5px 10px; padding:0"} [:b title]])

(defn make-footer
  "Creates a default footer with a simple text"
  []
  [:div {:class "footer" :style "width: 550px; text-align: right;"} 
   [:p {:style "color: #8899a6"} "Powered by Arquimedes &copy;"]])

(defn make-table-template 
  "Creates a default html view from an arbitrary source map
  
  * *source* arbitrary map to build a template"
  [source]
  [:table {:style "margin:5px 10px; border:2px solid #f5f8fa; padding:0"
           :cellpadding "5" :cellspacing "0" :width "550px"} 
   [:tbody (map-indexed (fn [i key] 
                          (let [style (if (even? i) 
                                        "background-color:#f5f8fa"
                                        "background-color:#ffffff")]
                            [:tr {:style style} 
                             [:th {:width "20%" :align "left"} (name key)]
                             [:td {:width "80%" :align "left"} (key source)]])) 
                        (keys source))]])

(defn event->html 
  "Produces a HTML representation of an event
  
  * *arp-event* to be represented in HTML
  * html-template (optional) to produce a representation of an event. This template uses 
  vectors to represent elements and is parsed using Hiccup and replaces 
  arp-event key ocurrences into vectors with its value"
  [arp-event & [html-template]]
  (if (or (not html-template) (nil? html-template) (empty? html-template))
    (make-table-template arp-event)
    (letfn [(catafixia [map event]                      
             (if (coll? event)
               (wlk/walk (partial catafixia map) identity event)
               (if-let [value (get map event)]
                 value
                 event)))]
     (wlk/walk (partial catafixia arp-event) identity html-template))))

(defn email-event
  "Sends an event or a sequence of events via email
   
   * *smtp-opts* SMTP options passed to Postal to send the email
   * *msg-opts*  Message options passed to Postal to send the email. Value of 
   :body is replaced with HTML produced by make-header, event->html and make-footer
   * *events* to be sended
   * keys (optional) keys to be sended 
   * html-template (optional) to produce a representation of an event"
  [smtp-opts msg-opts events & [keys html-template]]
  (let [title   (:subject msg-opts)
        events  (flatten [events])
        select    (if (or (not keys) (nil? keys) (empty? keys))
                    events 
                    (map (fn [x] (select-keys x keys)) events))
        resume  (map (fn [event] (event->html event html-template)) select)
        content (hiccup/html (make-header title) resume (make-footer))
        body    [{:type "text/html" :content content}]]
   (postal/send-message smtp-opts
                        (merge msg-opts {:body body}))))

(defn mailer
  "Returns a mailer, which is a function invoked with a map of options and 
  returns a stream that takes a single or a sequence of events, and sends an 
  email about them.

  Examples:

  ```
  ;; Mailer that uses a local sendmail
  (def email (mailer))

  ;; Mailer with mixed Postal options  
  (def email (mailer {:host 'smtp.gmail.com' 
                      :user 'sample@gmail.com'
                      :pass 'somesecret'
                      :subject 'Help!!'
                      :to ['ops@example.com' 'support@example.com']
                      :ssl :yes}))

  ;; Invoking email sending only :message and :ts fields
  (email event [:message :ts])

  ;; Invoking email sending only :message and :ts fields into a custom template
  (email event [:message :ts] [:p [:em :message] ' since ' [:i :ts]])
  ```  
  "
  ([] (mailer {}))
  ([opts]
   (let [smtp-keys [:host :port :user :pass :ssl :tls :sender]
         smtp-opts (select-keys opts smtp-keys)
         msg-opts  (select-keys opts (remove (set smtp-keys) (keys opts)))]
     (mailer smtp-opts msg-opts)))
  ([smtp-opts msg-opts]
   (let [msg-opts (merge {:from "arquimedes"} msg-opts)]
     (fn [e & [keys html-template]]
       (email-event smtp-opts msg-opts e keys html-template)))))
