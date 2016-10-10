(ns event-data-doi-reverser.core
  (:require [crossref.util.doi :as cr-doi]
            [event-data-doi-reverser.util :as util]
            [event-data-doi-reverser.crossref-md-api :as cr-md-api]
            [event-data-doi-reverser.handle :as handle]
            [event-data-doi-reverser.urls :as urls]
            [event-data-doi-reverser.storage :as storage])
  (:require [clojure.tools.logging :as log]
            [clojure.java.io :as io])
  (:require [clj-time.coerce :as coerce]
            [clj-time.core :as clj-time]
            [config.core :refer [env]]
            [korma.core :as k]
            [korma.db :as kdb]
            [liberator.core :as l]
            [liberator.representation :as representation]
            [compojure.core :as c]
            [compojure.route :as r]
            [org.httpkit.server :as server]
            [ring.middleware.params :refer [wrap-params]])
  (:import [java.io File])
  (:gen-class))

; Sometimes two DOIs refer to the same work, one each for the abstract and metadata.
(def meta-type-abstract 1)
(def meta-type-metadata 2)
(def meta-type-alias 3)

; The resource URL was invalid.
(def error-no-error 0)
(def error-bad-resource-url 1)
(def error-doi-does-not-resolve 2)

; DOI

(defn update-resource-urls-batch
  "Fetch and update a batch of items that don't have resource urls.
  TODO: Also do this by date?"
  []
  (let [counter (atom 0)
        input-items (k/select storage/items (k/where {:resource_url_updated nil}) (k/limit 1000))
        ; seq of futures toiling away in the background
        ; Takes approx 90 seconds for 1000 in parallell, ~ 11 per second
        ; Takes about 810 seconds for 10,000 in parallel, ~ 12 per second
        requests (doall (map #(vector % (handle/resolve-doi-link-async (:doi %))) input-items))]
    
    (doseq [request requests]
      (try 
        (let [[item async-response] request
              [resource-url alias-doi] @async-response]

          ; We get either a URL or an alias-doi. Do different things in each case.
          (cond 
            resource-url (let [; If resource URL doesn't parse, can be nil.
                               ; This can happen in error cases e.g. "10.5992/ajcs-d-12-00011.1" -> "ajcsonline.org/doi/abs/10.5992/AJCS-D-12-00011.1" has no scheme.
                               resource-url-domain (urls/try-get-host resource-url)
                               resource-url-domain-id (when resource-url-domain (storage/get-resource-url-domain-id resource-url-domain))]
                            
                            ; Item already exists in the database.
                            (k/update storage/items (k/where {:id (:id item)})
                                    (k/set-fields {:resource_url resource-url
                                                   :error_code (if resource-url-domain 0 error-bad-resource-url)
                                                   :resource_url_domain_id resource-url-domain-id
                                                   :resource_url_updated (coerce/to-sql-date (clj-time/now))})))
            
            alias-doi (let [alias-item-id (storage/ensure-item-get-id alias-doi)]
                        (k/update storage/items (k/where {:id (:id item)})
                          (k/set-fields {:resource_url nil
                                         :meta_type meta-type-alias
                                         :defer_to_item_id alias-item-id
                                         :resource_url_domain_id nil
                                         :resource_url_updated (coerce/to-sql-date (clj-time/now))})))
            
            ; If we can't get either, that means the DOI proxy didn't find it.
            :default (do
                       (k/update storage/items (k/where {:id (:id item)})
                                 (k/set-fields {:resource_url nil
                                                :error_code error-doi-does-not-resolve
                                                :resource_url_domain_id nil
                                                :resource_url_updated (clj-time/now)}))
                       
                       (log/error "Can't find either resource or URL for DOI" (:doi item)))))
          (swap! counter inc)
        (catch Exception e (do
                             (log/error request)
                             (log/error e)))))
    @counter))


; main

(defn main-update-items
  "Ingest all items from the Crossref Metadata API deposited/updated in the given range"
  [from-date until-date]
  (log/info "Main ingest" from-date until-date)
  (let [counter (atom 0)]
    (doseq [doi (cr-md-api/fetch-dois-from-mdapi-page from-date until-date)]
      (storage/ensure-item doi)
      (swap! counter inc)
      (when (zero? (mod @counter 1000))
        (log/info "Inserted" @counter "items")))))

(defn main-update-items-many
  "Ingest all items from the Crossref Metadata API for the given date strings in parallel."
  [dates]
  (let [threads (map #(new Thread (fn [] (main-update-items % %))) dates)]
    (doseq [thread threads]
      (log/info "Start...")
      (.start thread))
    
    (doseq [thread threads]
      (log/info "Wait...")
      (.join thread))))

(defn main-update-resource-urls
  "Update resource URLs for those that haven't been updated."
  []
  (loop []
    (let [num-updated (update-resource-urls-batch)]
      (log/info "Updated" num-updated "items' resource URL.")    
    (if-not (zero? num-updated)
      (recur)))))

; This many Item samples per domain for probing naive resource URLs.
(def naive-sample-size 20)

(defn sample-naive-redirect-urls-domain
  "Take a sample of Items for a given domain, follow naïve links and update the database."
  [domain-name domain-id]
  (log/info "Sample domain" domain-name)
  (let [total-items (-> (k/exec-raw ["SELECT COUNT(resource_url_domain_id) AS c FROM items WHERE resource_url_domain_id = ?" [domain-id]] :results) first :c)
        total-with-resource-urls (-> (k/exec-raw ["SELECT COUNT(resource_url_domain_id) AS c FROM items WHERE resource_url_domain_id = ? AND resource_url IS NOT NULL" [domain-id]] :results) first :c)
        unsampled-items (-> (k/exec-raw ["SELECT COUNT(resource_url_domain_id) AS c FROM items WHERE resource_url_domain_id = ? AND naive_destination_url_updated IS NULL AND resource_url IS NOT NULL" [domain-id]] :results) first :c)]
    (log/info "Total items:" total-items ", of which with resource urls," total-with-resource-urls ", of which yet unsampled:" unsampled-items)
    
    ; The resource_url_domain link is predicated on the resource_url field. Extra integrity check assertion.
    (when-not (= total-items total-with-resource-urls)
      (log/error "Found items for domain without resource_url"))

    ; Use :naive_destination_url_updated to indicate whether or not the sample has been taken.
    (let [sample-items (k/select storage/items
                                 (k/where {:resource_url_domain_id domain-id :naive_destination_url_updated nil})
                                 (k/where (not (nil? :resource_url)))
                                 (k/limit naive-sample-size))]
      (doseq [item sample-items]
        
        (let [destination-url (urls/resolve-link-naive (:resource_url item))]
          (log/info "Sample" (:doi item) "=" (:resource_url item) " => " destination-url)
          (k/update
            storage/items
            (k/set-fields {:naive_destination_url destination-url
                           :naive_destination_url_updated (clj-time/now)})
            (k/where {:id (:id item)})))))))

(defn main-sample-naive-redirect-urls
  "Scan a sample of URLs per resource url domain."
  []
  (log/info "Sample naïve redirect urls.")
  (let [domains (k/select storage/resource-url-domains)
        applied (pmap #(sample-naive-redirect-urls-domain (:domain %) (:id %)) domains)]
    (doall applied)))

(defn heuristic-items-duplicate-naive-destination-url
  "Mark duplicate naïve destination urls with the ID of the lowest one in the group.
  If there are dupes, this will run once for each, but it's idepotent and cheaper than the alternative."
  []
  (log/info "Update duplicate counts for Naive Destination URLs.")
  (k/exec-raw ["TRUNCATE working_count" []])
  (log/info "Page through Items...")
  (let [{min-id :min_id max-id :max_id} (-> (k/select :items (k/aggregate (min :id) :min_id) (k/aggregate (max :id) :max_id)) first)
        page-size 10000
        page-range (range min-id max-id page-size)]
    (doseq [offset page-range]
      (log/info "Update duplicates" offset (float (* 100 (/ offset (- max-id min-id)))) "%")
      (k/exec-raw [
        "INSERT INTO working_count (value, lowest_id, count) (SELECT naive_destination_url, id, 1 FROM items WHERE ID >= ? AND ID < ?) ON DUPLICATE KEY UPDATE count = count + 1, lowest_id = LEAST(lowest_id, items.id)"
        [offset (+ offset page-size)]])))
    (doseq [result (k/select :working_count (k/where (> :count 1)))]
      (log/info "Update duplicates for" (:value result) "item id" (:lowest_id result))
      (k/update :items (k/set-fields {:h_duplicate_naive_destination_url (:lowest_id result)}) (k/where {:naive_destination_url (:value result)}))))


(def deleted-resource-url "http://www.crossref.org/deleted_DOI.html")
(defn heuristic-item-all-deleted
  "Mark items that have been deleted."
  []
  (k/update
    :items
    (k/where {:resource_url deleted-resource-url})
    (k/set-fields {:h_deleted true})))

(defn heuristic-item-resource-equals-naive
  "Mark items where the resource URL is equal to the naïve destination URL"
  []
  (k/update
    :items
    (k/where (not= :naive_destination_url nil))
    (k/set-fields {:h_resource_equals_browser_destination_url false}))

  (k/update
    :items
    (k/where (not= :resource_url nil))
    (k/where (= :naive_destination_url :resource_url))
    (k/set-fields {:h_resource_equals_browser_destination_url true})))



(defn heuristic-items-duplicate-resource-url
  "Mark duplicate resource urls with the ID of the lowest one in the group."
  ; Uses a special table rather than GROUP because it's much much faster.
  []
  (log/info "Update duplicate counts for Resource URLs.")
  (k/exec-raw ["TRUNCATE working_count" []])
  (log/info "Page through Items...")
  (let [{min-id :min_id max-id :max_id} (-> (k/select :items (k/aggregate (min :id) :min_id) (k/aggregate (max :id) :max_id)) first)
        page-size 10000
        page-range (range min-id max-id page-size)]
    (doseq [offset page-range]
      (log/info "Update duplicates" offset (float (* 100 (/ offset (- max-id min-id)))) "%")
      (k/exec-raw [
        "INSERT INTO working_count (value, lowest_id, count) (SELECT resource_url, id, 1 FROM items WHERE ID >= ? AND ID < ?) ON DUPLICATE KEY UPDATE count = count + 1, lowest_id = LEAST(lowest_id, items.id)"
        [offset (+ offset page-size)]])))
    (doseq [result (k/select :working_count (k/where (> :count 1)))]
      (log/info "Update duplicates for" (:value result) "item id" (:lowest_id result))
      (k/update :items (k/set-fields {:h_duplicate_resource_url (:lowest_id result)}) (k/where {:resource_url (:value result)}))))


(defn heuristic-resource-url-proportions
  "Update heuristics on proportions of items per resource url."
  [resource-url-domain]
  (let [domain-id (:id resource-url-domain)

        ; Only interested where we've sampled naïve redirects. Could be zero.
        resource-url-item-count (-> (k/select :items
                          (k/where {:resource_url_domain_id domain-id})
                          (k/where (not= :naive_destination_url_updated nil))
                          (k/aggregate (count :id) :cnt)) first :cnt)

        count-resource-equals-naive (-> (k/select :items
                                      (k/where {:resource_url_domain_id domain-id
                                                :h_resource_equals_browser_destination_url true})
                                      (k/where (not= :naive_destination_url_updated nil))
                                      (k/aggregate (count :id) :cnt)) first :cnt)

        ; TODO browser url also
        proportion-resource-equals-naive (when-not (zero? resource-url-item-count)
                                                   (float (/ count-resource-equals-naive resource-url-item-count)))]

    ; (log/info "Domain " (:domain resource-url-domain) " proportion where Resource URL == naïve destination url:" proportion-resource-equals-naive)
    (k/update storage/resource-url-domains
      (k/where {:id domain-id})
      (k/set-fields {:h_proportion_naive_equals_browser_destination_url proportion-resource-equals-naive}))))

(defn main-derive-heuristics
  "Calculate various heuristics. Full scan."
  []
  ; Item heuristics

  ; One-off updates.
  (log/info "Updating all-items heuristics...")
  (heuristic-item-all-deleted)
  (heuristic-item-resource-equals-naive)

  (heuristic-items-duplicate-resource-url)
  (heuristic-items-duplicate-naive-destination-url)

  ; (log/info "Updating per-item heuristics...")
  ; none currently
  
  ; resource-url-domain heuristics
  (log/info "Updating resource-url-domain heuristics")
  (kdb/transaction
    (doseq [resource-url (k/select storage/resource-url-domains)]
      (heuristic-resource-url-proportions resource-url)))

  )

(def export-dir "/tmp/doi-reverser")

(defn export-all-dois
  "Export ALL DOIs, regardless of status."
  []
  (let [output-dir (new File export-dir)
        all-dois-f (new File output-dir "reverse-all-dois.txt")]
   (when-not (.isDirectory output-dir)
    (.mkdirs output-dir))
   (with-open [out-data (io/writer all-dois-f)]
    (doseq [item (storage/all-items)]
      (if-let [doi (:doi item)]
        (do
          (.write out-data doi)
          (.newLine out-data))
        (log/error "Error: Item without a DOI." item))))))

(defn main-export-all
  []
  "Export all artifact files."
  (export-all-dois)
  )

(defn lookup-item-from-resource-url
  [url]
  ; TODO look for canonical/primary DOI - follow links and check it's been vetted.
  (first (k/select storage/items (k/where {:resource_url url}))))

(defn try-reverse
  [url]
  (log/info "Look up" url)
  (if-let [item (lookup-item-from-resource-url url)]
     [:resource-url (:doi item)]
     nil))

(l/defresource server-reverse
 []
 :allowed-methods [:get]
 :available-media-types ["text/plain"]
 :malformed? (fn [ctx]
              (let [q (get-in ctx [:request :params "q"])]
                [(not q) {::q q}]))
 :exists? (fn [ctx]
            (let [result (try-reverse (::q ctx))
                  [method doi] result]
                (log/info "Query" (::q ctx) "->" method doi)
              (when result {::method method ::doi doi})))
 :handle-ok (fn [ctx]
              (representation/ring-response
                     {:status 200
                      :headers {
                        "X-Query" (::q ctx)
                        "X-Method" (name (::method ctx))}
                      :body (::doi ctx)})))

(l/defresource server-counts
  []
  :allowed-methods [:get]
  :available-media-types ["application/json"]
  :handle-ok (fn [ctx]
    {:items {; All known Items.
             :total (-> (k/select :items (k/aggregate (count :id) :cnt)) first :cnt)
             ; How many have a present resource URL.
             :with-resource-url (-> (k/select :items (k/where (not= :resource_url nil)) (k/aggregate (count :id) :cnt)) first :cnt)
             ; How many produced an error when trying to retrieve a resource URL.
             :with-resource-url-error (-> (k/select :items (k/where (in :error_code [error-bad-resource-url error-doi-does-not-resolve])) (k/aggregate (count :id) :cnt)) first :cnt)
             ; How many we have found the naïve destination.
             :with-naive-destination (-> (k/select :items (k/where (not= :naive_destination_url_updated nil)) (k/aggregate (count :id) :cnt)) first :cnt)}
     :item-heuristics {:duplicate-naive-destination-url (-> (k/select :items (k/where (not= :h_duplicate_naive_destination_url nil)) (k/aggregate (count :id) :cnt)) first :cnt)
                       :duplicate-resource-url (-> (k/select :items (k/where (not= :h_duplicate_resource_url nil)) (k/aggregate (count :id) :cnt)) first :cnt)
                       :deleted (-> (k/select :items (k/where (= :h_deleted true)) (k/aggregate (count :id) :cnt)) first :cnt)}
     :doi_prefixes {:total (-> (k/select :doi_prefixes (k/aggregate (count :id) :cnt)) first :cnt)}
     :resource-url-domains {:total (-> (k/select :resource_url_domains (k/aggregate (count :id) :cnt)) first :cnt)}}))

(l/defresource server-domains-counts
  []
  :allowed-methods [:get]
  :available-media-types ["application/json"]
  :handle-ok (fn [ctx]
    ; TODO add count of samples.
    (k/select storage/resource-url-domains)))

(c/defroutes routes
  (c/GET "/status/counts" [] (server-counts))
  (c/GET "/status/domains" [] (server-domains-counts))
  (c/GET "/reverse" [] (server-reverse))
  ;; backward compatibility
  (c/GET "/guess-doi" [] (server-reverse)))

(def app
  (-> routes
      (wrap-params)))

(defn main-run-server
  []
  (server/run-server app {:port (Integer/parseInt (:port env))}))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (let [command (first args)]
    (log/info "Command:" command)
    (condp = command
      "update-items" (main-update-items (second args) (second args))
      "update-items-many" (main-update-items-many (rest args))
      "update-resource-urls" (main-update-resource-urls)
      "sample-naive-redirect-urls" (main-sample-naive-redirect-urls)
      "derive-heuristics" (main-derive-heuristics)
      "export" (main-export-all)

      "server" (main-run-server)))
  (shutdown-agents))

