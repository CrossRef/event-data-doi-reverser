(ns event-data-doi-reverser.urls
  (:require [org.httpkit.client :as http]
            [robert.bruce :refer [try-try-again]]
            [clojure.java.shell :refer [sh]]
            [clojure.tools.logging :as log]
            [clojure.data.json :as json])
  (:import [java.net URLEncoder URL]))

(def headers {
  "User-Agent" "Crossref Event Data labs@crossref.org (+http://eventdata.crossref.org)"
  "Referer" "http://eventdata.crossref.org/bot"})

(defn resolve-link-naive
  "Follow a URL to its destination using simple Location headers."
  [url]
  (let [result (try-try-again
                 {:sleep 5000 :tries 2}
                 #(-> url (http/get {:follow-links true :headers headers}) deref))
        url (-> result :opts :url)
        has-err (boolean (-> result :opts :error))]
    [url has-err]))

(defn try-get-host
  "Return the hostname of a URL string or nil on error."
  [url-str]
  (try
    (.getHost (new URL url-str))
    (catch java.net.MalformedURLException e
      nil)))

(defn resolve-link-browser
  "Follow a URL to its destination using headless browser. Return nil on error."
  [url]
  (log/info "Resolve browser:" url)
  (let [response (sh "timeout" "10" "phantomjs" "etc/fetch.js" :in url)]
    (if-not (= (:exit response) 0)
      (do
        (log/error "Error calling Phantom exit:" (:exit response))
        (log/error "Error calling Phantom STDOUT:" (:err response))
        (log/error "Error calling Phantom STDERR:" (:out response))
        nil)
      (try
        (let [output (json/read-str (:out response))
          [last-url last-code] (last (output "path"))]
          [last-url last-code])
        (catch Exception e (log/info "Exception processing browser response for:" url ", got response" (:out response) ", exception:" e ))))))

