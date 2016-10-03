(ns event-data-doi-reverser.storage
  (:require [crossref.util.doi :as cr-doi])
  (:require [clj-time.coerce :as coerce]
            [config.core :refer [env]]
            [korma.core :as k]
            [korma.db :as kdb])
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
    ))


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
    :resource_url_unique
    
    ; The meta-type of the Item. Useful for disambiguation.
    :meta_type
    
    ; When this is a secondary (e.g. meta-type abstract), the primary item (e.g. meta-type metadata).
    :defer_to_item_id

    ; The destination URL after naive redirects.
    :naive_destination_url
    :naive_destination_url_updated

    ; TODO :naive_redirect
    ; TODO :browser_redirect
    ; TODO date also

    ; Naive redirect probe
    ; 0 - not probed, 1 - unreachable, 2 - naive = browser, 3 - naive ≠ browser
    )

  (k/prepare (fn [{resource_url_updated :resource_url_updated
                     naive_destination_url_updated :naive_destination_url_updated
                     :as obj}]
                 (assoc obj
                  :resource_url_updated (when resource_url_updated (coerce/to-sql-date resource_url_updated))
                  :naive_destination_url_updated (when naive_destination_url_updated (coerce/to-sql-date naive_destination_url_updated)))))
  
  (k/transform (fn [{resource_url_updated :resource_url_updated
                     naive_destination_url_updated :naive_destination_url_updated
                     :as obj}]
                 (assoc obj
                  :resource_url_updated (when resource_url_updated (str (coerce/from-sql-date resource_url_updated)))
                  :naive_destination_url_updated (when naive_destination_url_updated (str (coerce/from-sql-date naive_destination_url_updated)))))))


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

