(ns schema-compliance.core
  (:require [clojure.data.json :as json])
  (:use [clojure.inspector :only [inspect inspect-tree]]))

(defrecord Err [exception data])

(defn read [src]
  (->
   src
   (slurp)
   (clojure.string/split #"\n")))

(defn summarize-read [d summary]
  (swap! summary assoc :read {:total (count d)})
  d)

(defn line->json [l]
  (try
    (json/read-str l)
    (catch Exception e (Err. e l))))

(defn error? [d]
  (instance? Err d))

(defn basic-check [lines]
  (->>
   lines 
   (pmap line->json)
   (group-by (complement error?))))

(defn filter-ok [d]
  (or (get d true) []))

(defn filter-nok [d]
  (or (get d false) []))

(defn summarize-basic-check [d summary]
  (swap! summary assoc
         :basic-check
         {:ok (count (filter-ok d))
          :nok  (count (filter-nok d))})
  d)

(defn get-source-client [d]
  (get d "sourceClient"))

(defn get-event-type [d]
  (get d "eventType"))

(defn get-schema [d]
  (get-in d ["metadata" "schema"]))

(defn compliant? [d]
  ((complement nil?) (get-schema d)))

(defn compliance-check [d]
  (group-by compliant? d)) 

(defn summarize-compliance-check [d summary]
  (let [ok (filter-ok d)
        nok (filter-nok d)
        schemas (distinct (map #(get-schema %) ok))]
    (swap! summary assoc :compliance-check {:ok (count ok)
                                            :ok-details {:schemas schemas}
                                            :nok (count nok)
                                            :nok-details {:events
                                                          (->> nok
                                                               (group-by get-source-client)
                                                               (vec)
                                                               (map (fn [[source-client events]]
                                                                      {source-client (frequencies (map #(get-event-type %) events))}))
                                                               (reduce merge))}}))
  d)

(defn summarize 
  ([summary] @summary)
  ([_ summary] (summarize summary)))


(def summary (let [summary (atom {})]
               (->
                src
                (read)
                (summarize-read summary)
                (basic-check)
                (summarize-basic-check summary)
                (filter-ok)
                (compliance-check)
                (summarize-compliance-check summary)
                (summarize summary))))


;(def src "TODO")

;(spit "TODO" (json/write-str summary :escape-slash false))
