(ns event-data-doi-reverser.crossref-md-api
  "Crossref Metadata API interfacing."
  (:require [crossref.util.doi :as cr-doi]
            [event-data-doi-reverser.util :as util])
  (:require [clojure.tools.logging :as log]
            [clojure.java.io :refer [reader]]
            [clojure.data.json :as json])
  (:require [clj-time.coerce :as coerce]
            [clj-time.core :as clj-time]
            [throttler.core :refer [throttle-fn]]
            [org.httpkit.client :as http]
            [config.core :refer [env]]
            [korma.core :as k]
            [korma.db :as kdb]
            [robert.bruce :refer [try-try-again]]
            [liberator.core :as l]
            [liberator.representation :as representation]
            [compojure.core :as c]
            [compojure.route :as r]
            [org.httpkit.server :as server]
            [ring.middleware.params :refer [wrap-params]])
  (:import [java.net URLEncoder URL])
  (:gen-class))


; MDAPI

; Optimum size: fetch 2000 items
; 100: 206 seconds
; 500: 70 seconds
; 1000: 92 seconds
(def api-page-size 500)

(defn fetch-mdapi-page
  "Fetch a page of API results for works in the given date range."
  [from-date until-date cursor]
  (log/info "Cursor" from-date until-date cursor)
  (let [filter-param (str "from-deposit-date:" from-date ",until-deposit-date:" until-date)
        result @(http/get "http://api.crossref.org/v1/works"
                          {:as :stream
                           :query-params {:cursor cursor
                                          :rows api-page-size
                                          :filter filter-param}})]
    (json/read (reader (:body result)) :key-fn keyword)))


(defn fetch-mdapi-pages
  "Lazy sequence of pages for the date range"
  ([from-date until-date]
    (fetch-mdapi-pages from-date until-date "*"))
  
  ([from-date until-date cursor]
    (let [result (fetch-mdapi-page from-date until-date cursor)
          next-token (-> result :message :next-cursor)
          ; We always get a next token. Empty page signals the end of iteration.
          finished (empty? (-> result :message :items))]
      
      (if finished
        [result]
        (lazy-seq (cons result (fetch-mdapi-pages from-date until-date next-token)))))))
