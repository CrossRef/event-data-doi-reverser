(ns event-data-doi-reverser.urls
  (:require [org.httpkit.client :as http]
            [robert.bruce :refer [try-try-again]])
  (:import [java.net URLEncoder URL]))

(defn resolve-link-naive
  "Follow a URL to its destination using simple Location headers."
  [url]
  (let [result (try-try-again
                 {:sleep 5000 :tries 10}
                 #(-> url (http/get {:follow-links true}) deref))
        url (-> result :opts :url)]
    
    (when url
      url)))

(defn try-get-host
  "Return the hostname of a URL string or nil on error."
  [url-str]
  (try
    (.getHost (new URL url-str))
    (catch java.net.MalformedURLException e
      nil)))
