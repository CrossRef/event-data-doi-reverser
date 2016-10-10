(ns event-data-doi-reverser.storage
  (:require [crossref.util.doi :as cr-doi]
            [event-data-doi-reverser.util :as util])
  (:require [clj-time.coerce :as coerce]
            [config.core :refer [env]]
            [korma.core :as k]
            [korma.db :as kdb]
            [clojure.tools.logging :as log])
  (:gen-class))

(kdb/defdb db (kdb/mysql {:db (:db-name env)
                          :host (:db-host env) 
                          :port (Integer/parseInt (:db-port env))
                          :user (:db-user env)
                          :password (:db-password env)}))

; Prefix used in a DOI.
(k/defentity doi-prefixes
  (k/table "doi_prefixes")
  (k/pk :id)
  (k/entity-fields
    :id
    :prefix))

; Domain used in a Resource URL.
(k/defentity resource-url-domains
  (k/table "resource_url_domains")
  (k/pk :id)
  (k/entity-fields
    :id
    :domain

    ; Heuristics
    ; Taken from a sample. Can be null if there is no data.

    ; Proportion of Items with this resource URL where the resource URL is the same as the naïve destination url.
    :h_proportion_resource_equals_naive_destination_url

    ; Proportion of Items with this resource URL where the resource URL is the same as the browser destination url.
    :h_proportion_resource_equals_browser_destination_url

    ; Proportion of Items with this resource URL where the link URL is the same as the browser url.
    :h_proportion_naive_equals_browser_destination_url))


(k/defentity items
  (k/table "items")
  (k/pk :id)
  (k/entity-fields
    :id
    :prefix_id
    :doi
    
    ; The Resource URL, if known
    :resource_url
    
    ; The ID of the resource domain, if known.
    :resource_url_domain_id
    
    ; Last time the resource URL was updated.
    :resource_url_updated
    
    :error_code
    
    ; Have we checked that the resource URL is unique?
    :checked_resource_url_unique
    
    ; Is the resource URL unique? If not we can't use the URL or any data deriving from it.
    ; :resource_url_unique ; TODO REMOVE
    
    ; The meta-type of the Item. Useful for disambiguation.
    :meta_type
    
    ; When this is a secondary (e.g. meta-type abstract), the primary item (e.g. meta-type metadata).
    :defer_to_item_id

    ; The destination URL after naive redirects.
    :naive_destination_url
    :naive_destination_url_updated

    ; Heuristics
    ; These are denormalized and some are materialized views. But there will be up to 100 million rows.

    ; Does the destination URL have duplicates in other Items? If so, this contains the lowest ID of all other duplicates.
    :h_duplicate_naive_destination_url

    ; Does the resource URL have duplicates in other Items? If so this contains the lowest ID fo other duplicates.
    :h_duplicate_resource_url

    ; Have we deleted the item?
    :h_deleted

    ; Proportion of Items with this resource URL where the resource URL is the same as the browser destination url.
    :h_resource_equals_browser_destination_url

    ; Proportion of Items with this resource URL where the link URL is the same as the browser url.
    :h_naive_equals_browser_destination_url)


  (k/prepare (partial util/map-keys {:resource_url_updated coerce/to-sql-date :naive_destination_url_updated coerce/to-sql-date}))
  (k/transform (partial util/map-keys {:resource_url_updated coerce/from-sql-date :naive_destination_url_updated coerce/from-sql-date})))


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
                (k/select doi-prefixes (k/where {:prefix prefix}))
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
                (k/select resource-url-domains (k/where {:domain domain}))
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
    (if-let [id (-> (k/select items (k/where {:doi doi})) first :id)]
      id
      (do
        (ensure-item doi)
        (-> (k/select items (k/where {:doi doi})) first :id)))))

(def page-size 10000)

(defn all-items
  ([] (all-items 0))
  ([id]
    (log/info "Fetch id" id "limit" page-size)
    (let [results (k/select items 
      (k/order :id :ASC) 
      (k/where (>= :id id)) (k/limit page-size))
          top-id (-> results last :id)]
      (if (empty? results)
        results
        (lazy-cat results (all-items (inc top-id)))))))

(defn all-items-nil-field
  "Return data set of all items where the named field is not null."
  ([field] (all-items-nil-field field 0))
  ([field id]
    (log/info "Fetch id" id "limit" page-size)
    (let [results (k/select items 
      (k/order :id :ASC) 
      (k/where (>= :id id))
      (k/where (= field nil))
      (k/limit page-size))
          top-id (-> results last :id)]
      (if (empty? results)
        results
        (lazy-cat results (all-items-nil-field field (inc top-id)))))))


