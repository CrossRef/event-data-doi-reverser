(ns event-data-doi-reverser.util-test
  (:require [clojure.test :refer :all]
            [event-data-doi-reverser.util :as util]))

(deftest map-keys
  (testing "map-keys applies functions"
    (is (= (util/map-keys
            {2 #(.toUpperCase %)
             :four (partial * 2)
             :five inc}
            {1 "one"
             2 "two"
             3 "three"
             :four 4
             :five 5
             :six 6})
          {1 "one"
           2 "TWO"
           3 "three"
           :four 8
           :five 6
           :six 6}))))
