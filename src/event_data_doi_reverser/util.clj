(ns event-data-doi-reverser.util
  (:require [org.httpkit.client :as http]
            [robert.bruce :refer [try-try-again]]))

(defn lazy-cat' [colls]
  (lazy-seq
    (if (seq colls)
      (concat (first colls) (lazy-cat' (next colls))))))

(defn map-keys
  "Accept an input dictionary and a dictionary of key names to tranform functions. Apply them."
  [transforms input-dict]
  (into {}
    (map (fn [[k v]]
      (if-let [f (transforms k)]
        [k (f v)]
        [k v])) input-dict)))