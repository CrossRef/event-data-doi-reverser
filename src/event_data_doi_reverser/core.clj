(ns event-data-doi-reverser.core
  (:require [crossref.util.doi :as cr-doi]
            [event-data-doi-reverser.util :as util]
            [event-data-doi-reverser.crossref-md-api :as cr-md-api]
            [event-data-doi-reverser.handle :as handle]
            [event-data-doi-reverser.urls :as urls]
            [event-data-doi-reverser.storage :as storage])
  (:require [clojure.tools.logging :as log])
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
  (:gen-class))

; Sometimes two DOIs refer to the same work, one each for the abstract and metadata.
(def meta-type-abstract 1)
(def meta-type-metadata 2)
(def meta-type-alias 3)

; The resource URL was invalid.
(def error-no-error 0)
(def error-bad-resource-url 1)
(def error-doi-does-not-resolve 2)

; Cache of prefix -> prefix id.
(def prefix-ids (atom {}))

(defn get-prefix-id
  "Lookup the ID for a DOI prefix."
  [prefix]
  (if-let [prefix-id (@prefix-ids prefix)]
    prefix-id
    (do
      (k/exec-raw ["INSERT IGNORE INTO doi_prefixes (prefix) VALUES (?)" [prefix]])
      (let [id (->
                (k/select storage/doi-prefixes (k/where {:prefix prefix}))
                first
                :id)]
      (swap! prefix-ids assoc prefix id)
      id))))

; Cache of resource domain -> resource domain id
(def resource-url-domain-ids (atom {}))

(defn get-resource-url-domain-id
  "Lookup the ID for a DOI resource domain."
  [domain]
  (if-let [domain-id (@resource-url-domain-ids domain)]
    domain-id
    (do
      (k/exec-raw ["INSERT IGNORE INTO resource_url_domains (domain) VALUES (?)" [domain]])
      (let [id (->
                (k/select storage/resource-url-domains (k/where {:domain domain}))
                first
                :id)]
      (swap! resource-url-domain-ids assoc domain id)
      id))))

(defn ensure-item
  "Ensure an item exits by its DOI."
  [doi]
  (let [doi (.toLowerCase (cr-doi/non-url-doi doi))
        prefix (cr-doi/get-prefix doi)
        prefix-id (get-prefix-id prefix)]
    (k/exec-raw ["INSERT IGNORE INTO items (prefix_id, doi) VALUES (?,?)" [prefix-id doi]])))

(defn ensure-item-get-id
  "Ensure an item exists by its DOI, return its ID."
  [doi]
  (let [doi (.toLowerCase doi)]
    (if-let [id (-> (k/select storage/items (k/where {:doi doi})) first :id)]
      id
      (do
        (ensure-item doi)
        (-> (k/select storage/items (k/where {:doi doi})) first :id)))))


(defn fetch-dois-from-mdapi-page
  "Fetch lazy sequence of DOIs updated within the two dates."
  [from-date until-date]
  (let [pages (cr-md-api/fetch-mdapi-pages from-date until-date)
        ; Pages of DOIs
        doi-pages (map (fn [page]
                         (map :DOI (-> page :message :items))) pages)
        ; Lazy seq of the all DOIs across pages.
        dois (util/lazy-cat' doi-pages)
        
        ; Sometimes the DOI is missing or blank.
        only-dois (remove empty? dois)]
    only-dois))

; DOI

(defn update-resource-urls-batch
  "Fetch and update a batch of items that don't have resource urls.
  TODO: Also do this by date?"
  []
  (let [counter (atom 0)
        input-items (k/select storage/items (k/where {:resource_url_updated nil}) (k/limit 1000))
        ; seq of futures toiling away in the background
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
                               resource-url-domain-id (when resource-url-domain (get-resource-url-domain-id resource-url-domain))]
                            ; (log/info "DOI resource" (:doi item) "->" resource-url)
                            
                            ; Item already exists in the database.
                            (k/update storage/items (k/where {:id (:id item)})
                                    (k/set-fields {:resource_url resource-url
                                                   :error_code (if resource-url-domain 0 error-bad-resource-url)
                                                   :resource_url_domain_id resource-url-domain-id
                                                   :resource_url_updated (coerce/to-sql-date (clj-time/now))})))
            
            alias-doi (let [alias-item-id (ensure-item-get-id alias-doi)]
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
                                                :resource_url_updated (coerce/to-sql-date (clj-time/now))}))
                       
                       (log/error "Can't find either resource or URL for DOI" (:doi item)))))
          (swap! counter inc)
        (catch Exception e (do
                             (log/error request)
                             (log/error e)))))
    @counter))


; main

(defn main-update-items
  "Ingest all items updated in the given range"
  [from-date until-date]
  (log/info "Main ingest" from-date until-date)
  (let [counter (atom 0)]
    (doseq [doi (fetch-dois-from-mdapi-page from-date until-date)]
      (ensure-item doi)
      (swap! counter inc)
      (when (zero? (mod @counter 1000))
        (log/info "Inserted" @counter "items")))))

(defn main-update-items-many
  "Ingest all items for the given date strings in parallel."
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

(defn main-scan-unique-resource-urls
  "Scan all resource URLs to check that they're unique."
  []
  ; TODO 
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

(c/defroutes routes
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

      "server" (main-run-server)))
  (shutdown-agents))

