(defproject event-data-doi-reverser "0.1.0-SNAPSHOT"
  :description "Service for reversing landing pages into DOIs."
  :url "http://eventdata.crossref.org"
  :license {:name "The MIT License (MIT)"
            :url "https://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/tools.nrepl "0.2.11"]
                 [crossref-util "0.1.13"]
                 [yogthos/config "0.8"]
                 [throttler "1.0.0"]
                 [http-kit "2.1.18"]
                 [org.clojure/java.jdbc "0.4.2"]
                 [mysql-java "5.1.21"] 
                 [korma "0.4.0"]
                 [robert/bruce "0.8.0"]
                 [compojure "1.5.1"]
                 [liberator "0.14.1"]]
  :main ^:skip-aot event-data-doi-reverser.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}
             :prod {:resource-paths ["config/prod"]}
             :dev  {:resource-paths ["config/dev"]}})
