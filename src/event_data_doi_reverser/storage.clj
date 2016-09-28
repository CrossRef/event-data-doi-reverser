(ns event-data-doi-reverser.storage
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
    :domain))

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
    :defer_to_item_id)
  
  (k/transform (fn [{resource_url_updated :resource_url_updated :as obj}]
                 (if-not resource_url_updated obj
                   (assoc obj :resource_url_updated (str (coerce/from-sql-date resource_url_updated)))))))
