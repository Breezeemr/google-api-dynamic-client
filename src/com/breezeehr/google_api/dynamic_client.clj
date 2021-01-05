(ns com.breezeehr.google-api.dynamic-client
  (:require [cheshire.core :as json]
            [com.breezeehr.google-api.java-auth
             :refer [add-auth init-client]]
            [com.breezeehr.google-api.boostrap
             :as bootstrap]
            [aleph.http :as http]
            [cemerick.url :as url]
            [clojure.string :as str]))


(defn fast-select-keys [ks]
  (let [ks (into [] (map keyword) ks)]
    (fn [m]
      (->
        (reduce
          (fn [acc k]
            (if-some [val (find m k)]
              (conj! acc val)
              acc))
          (transient {})
          ks)
        persistent!
        (with-meta (meta map))))))

(defn make-path-fn [baseUrl path path-params id]
  (let [path-ops (into [(fn [^StringBuffer acc values]
                          (.append acc baseUrl))]
                       (comp
                         (mapcat #(clojure.string/split % #"\}"))
                         (map (fn [x]
                                (if-some [path-param (get path-params x)]
                                  (let [k (keyword x)]
                                    (fn [^StringBuffer acc values]
                                      (let [v (k values)]
                                        (assert v (str "your input " (pr-str values) " must contains key " k " for " id " path."))
                                        (.append acc v))))
                                  (fn [^StringBuffer acc values]
                                    (.append acc x))))))
                       (clojure.string/split path #"\{"))]
    ;(prn path path-ops)
    (fn [vals]
      (.toString (reduce
                   (fn [acc bfn]
                     (bfn acc vals))
                   (StringBuffer.)
                   path-ops)))))

(defn make-method [{:strs [baseUrl] :as api-discovery}
                   upper-parameters
                   {:strs [httpMethod path parameters request id]
                    :as   method-discovery}]
  (let [parameters (into upper-parameters parameters)
        init-map     {:method httpMethod
                      :as     :json}
        path-params  (into {} (filter (comp #(= % "path") #(get % "location") val)) parameters)
        path-fn      (make-path-fn baseUrl path path-params id)
        query-params (into {} (filter (comp #(= % "query") #(get % "location") val)) parameters)
        query-ks     (into [] (map key) query-params)
        key-sel-fn   (fast-select-keys query-ks)]
    ;(prn (keys method-discovery))
    [id {:id          id
         :description (get method-discovery "description")
         :request
                      (fn [client op]
                        (-> init-map
                            (assoc :url (path-fn op))
                            (add-auth client)
                            (assoc :query-params (key-sel-fn op)
                                   :throw-exceptions false)
                            (cond->
                              request (assoc :body (let [enc-body (:request op)]
                                                     (assert enc-body (str "Request cannot be nil for operation " (:op op)))
                                                     (cheshire.core/generate-string enc-body))))
                            ;(doto prn)
                            ))}]))

(defn prepare-methods [api-discovery parameters methods]
  (reduce-kv
    (fn [acc k method]
      (conj acc (make-method api-discovery parameters method)))
    {}
    methods))

(defn prepare-resources [api-discovery parameters resources]
  (reduce-kv
    (fn [acc k {:strs [methods resources]}]
      (cond-> (into acc (prepare-methods api-discovery  parameters methods))
              (not-empty resources) (into (prepare-resources api-discovery parameters resources)))
      )
    {}
    resources))
(defn prefered-discovery [discovery-docs]
  (reduce (fn [acc v]
            (when (get v "preferred")
              (reduced v)))
          nil
          discovery-docs))

(defn discovery-matching-version [version discovery-docs]
  (reduce (fn [acc v]
            (when (= (get v "version") version)
              (reduced v)))
          nil
          discovery-docs))

(defn dynamic-create-client
  ([config api]
   (let [client (init-client config)
         discovery-ref
         (prefered-discovery
           (bootstrap/list-apis
             client
             {:name    api
              "fields" "items.discoveryRestUrl,items.name,items.version,items.preferred"}))
         _      (assert discovery-ref (str "api " api " not found"))
         api-discovery
                (bootstrap/get-discovery client discovery-ref)]
     (assoc client
       :api api
       :ops (prepare-resources
              api-discovery
              (or (get api-discovery "parameters") [])
              (get api-discovery "resources")))))
  ([config api version]
   (let [client (init-client config)
         discovery-ref
         (discovery-matching-version
           version
           (bootstrap/list-apis
             client
             {:name    api
              "fields" "items.discoveryRestUrl,items.name,items.version"}))
         _      (assert discovery-ref (str "api " api " version " version " not found"))
         api-discovery
                (bootstrap/get-discovery client discovery-ref)]
     (assoc client
       :api api
       :version version
       :ops (prepare-resources
              api-discovery
              (or (get api-discovery "parameters") [])
              (get api-discovery "resources"))))))

(defn ops [client]
  (run!
    (fn [[id {:keys [description]}]]
      (println "* " id)
      (print description)
      (println \newline))
    (->> client :ops
         (sort-by key))))
(defn request [client {:keys [op] :as operation}]
  (let [reqfn (-> client :ops (get op) :request)]
    (assert reqfn (str op
                      " is not implemented in "
                      (:api client)
                      (:version client)))
    (reqfn client operation)))

(defn invoke [client operation]
  (let [req (request client operation)]
    (http/request req)))



(comment

  (def storage-client (dynamic-create-client {} "storage"))
  (deref (invoke storage-client {:op      "storage.buckets.list"
                                 :project "breezeehr.com:breeze-ehr"}))
  (request storage-client {:op      "storage.buckets.list"
                           :project "breezeehr.com:breeze-ehr"})
  (request storage-client {:op      "storage.buckets.get"
                           :bucket "umls"
                           :project "breezeehr.com:breeze-ehr"})


  (ops storage-client))
