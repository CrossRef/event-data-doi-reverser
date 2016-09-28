(ns event-data-doi-reverser.util
  (:require [org.httpkit.client :as http]
            [robert.bruce :refer [try-try-again]]))

(defn lazy-cat' [colls]
  (lazy-seq
    (if (seq colls)
      (concat (first colls) (lazy-cat' (next colls))))))

