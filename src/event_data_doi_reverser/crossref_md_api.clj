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
(def api-page-size 1000)

(defn fetch-mdapi-page
  "Fetch a page of API results for works in the given date range."
  [from-date until-date cursor]
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
    (fetch-mdapi-pages from-date until-date "*" 0))
  
  ([from-date until-date cursor current-total]
    (let [result (try-try-again {:sleep 5000 :tries 10} #(fetch-mdapi-page from-date until-date cursor))
          next-token (-> result :message :next-cursor)
          ; We always get a next token. Empty page signals the end of iteration.
          items (-> result :message :items)
          finished (empty? items)
          num-page-results (count items)
          total-available (-> result :message :total-results)]
      
      (log/info "Cursor" from-date until-date cursor ". Got" current-total "/" total-available "=" (float (* 100 (/ current-total total-available))) "%")

      (if finished
        [result]
        (lazy-seq (cons result (fetch-mdapi-pages from-date until-date next-token (+ current-total num-page-results))))))))


(defn fetch-dois-from-mdapi-page
  "Fetch lazy sequence of DOIs updated within the two dates."
  [from-date until-date]
  (let [pages (fetch-mdapi-pages from-date until-date)
        ; Pages of DOIs
        doi-pages (map (fn [page]
                         (map :DOI (-> page :message :items))) pages)
        ; Lazy seq of the all DOIs across pages.
        dois (util/lazy-cat' doi-pages)
        
        ; Sometimes the DOI is missing or blank.
        only-dois (remove empty? dois)]
    only-dois))

