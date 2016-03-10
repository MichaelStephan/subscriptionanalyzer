(ns schema-compliance.core
  (:require [clojure.data.json :as json])
  (:use [clojure.inspector :only [inspect inspect-tree]]))

(defn read [src]
  (->
   src
   slurp
   (json/read-str :key-fn keyword)))

(defn ->list [subscriptions]
  (for [{:keys [subscriber provider package type validUntil]} subscriptions
        :when (nil? validUntil)]
    [(:id subscriber) (:type subscriber) (:id provider) (:id package) (:name package) type]))

(defn ->csv [listed-subscriptions]
  (->> listed-subscriptions 
       (map (fn [line] (clojure.string/join ";" line)))
       (clojure.string/join "\n")))

(defn ->dot [subscriptions]
  (->>
    (for [{:keys [subscriber provider package type validUntil]} subscriptions
          :when (nil? validUntil)]
      (str "\"" (:id subscriber) "\" -> "
           "\"" (:name package) "\n" (clojure.string/replace (:id package) #"-" "") "," (:id provider)  "\" [label=\"" type "\"];"))
    (clojure.string/join "\n")))

(defn wrap-dot-content [content]
  (str "digraph g {\n" "ranksep=30;\n ratio=auto;\n" content "\n}"))

(defn -main []
  (let [subscriptions (->> "all_subscriptions_prod_orghybris.json"
                           read)]
    (->> subscriptions
         ->list
         ->csv
         (str "subscriber name;subscriber type;provider name;package id;package name;subscription type\n")
         (spit "all_subscriptions_prod_orghybris.csv"))
    
    (->> subscriptions
         ->dot
         wrap-dot-content
         (spit "all_subscriptions_prod_orghybris.dot"))))

(-main)
