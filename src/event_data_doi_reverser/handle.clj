(ns event-data-doi-reverser.handle
  "Handle and DOI interfacing."
  (:require [clojure.tools.logging :as log]
            [clojure.data.json :as json])
  (:require [org.httpkit.client :as http]
            [robert.bruce :refer [try-try-again]])
  (:import [java.net URLEncoder URL])
  (:gen-class))


(defn resolve-doi-link-async
  "Resolve the URL from the DOI to find the resource url or the alias DOI. 
  Return tuple [resource-url alias-doi]. Only one is non-nil."
  [doi]
  (future
    (let [url (str "http://doi.org/api/handles/" (URLEncoder/encode doi "UTF-8"))
          values (try-try-again {:sleep 5000 :tries 10} #(-> url http/get deref :body (json/read-str :key-fn keyword) :values))
          alias-value (-> (filter #(= (:type %) "HS_ALIAS") values) first :data :value)
          url-value (-> (filter #(= (:type %) "URL") values) first :data :value)]
      (if alias-value
        [nil alias-value]
        [url-value nil]))))
