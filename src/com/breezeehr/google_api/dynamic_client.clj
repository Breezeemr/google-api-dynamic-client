(ns com.breezeehr.google-api.dynamic-client
  (:require [cheshire.core :as json]
            [com.breezeehr.google-api.java-auth
             :refer [add-auth init-client]]
            [com.breezeehr.google-api.boostrap
             :refer [list-apis get-discovery]]
            [aleph.http :as http]
            [cemerick.url :as url]
            [clojure.string :as str]))


(defn fast-select-keys [map ks]
  (->
    (reduce
      (fn [acc k]
        (if-some [val (find map k)]
          (conj! acc val)
          acc))
      (transient {})
      ks)
    persistent!
    (with-meta (meta map))))

(def lastmessage (atom nil))


(defn make-path-fn [path path-params id]
  (fn [vals]
    (reduce-kv
      (fn [acc parameter v]
        (let [nv (get vals parameter)]
          (assert nv (str "your input " (pr-str vals) " must contains key " parameter " for " id " path."))
          (clojure.string/replace
            acc
            (str "{" (name parameter) "}")
            nv)))
      path
      path-params)))

(defn make-method [{:keys [baseUrl] :as api-discovery}
                   upper-parameters
                   {:keys [httpMethod path parameters request id]
                    :as   method-discovery}]
  (let [parameters (into upper-parameters parameters)
        init-map     {:method httpMethod
                      :as     :json}
        path-params  (into {} (filter (comp #(= % "path") :location val)) parameters)
        path-fn      (make-path-fn path path-params id)
        query-params (into {} (filter (comp #(= % "query") :location val)) parameters)
        query-ks     (into [] (map key) query-params)
        key-sel-fn   (fn [m]
                       (fast-select-keys m query-ks))]
    ;(prn (keys method-discovery))
    [id {:id          id
         :description (:description method-discovery)
         :preform
                      (fn [client op]
                        (-> init-map
                            (assoc :url (str baseUrl (path-fn op)))
                            (add-auth client)
                            (assoc :query-params (key-sel-fn op)
                                   ; :aleph/save-request-message lastmessage
                                   :throw-exceptions false)
                            (cond->
                                request (assoc :body (let [enc-body (:request op)]
                                                       (assert enc-body (str "Request cannot be nil for operation "  (:op op)))
                                                       (cheshire.core/generate-string enc-body))))
                            ;(doto prn)
                            http/request))}]))

(defn prepare-methods [api-discovery parameters methods]
  (reduce-kv
    (fn [acc k method]
      (conj acc (make-method api-discovery parameters method)))
    {}
    methods))

(defn prepare-resources [api-discovery parameters resources]
  (reduce-kv
    (fn [acc k {:keys [methods resources]}]
      (cond-> (into acc (prepare-methods api-discovery  parameters methods))
              (not-empty resources) (into (prepare-resources api-discovery parameters resources)))
      )
    {}
    resources))

(defn dynamic-create-client
  ([config api]
   (let [client (init-client config)
         discovery-ref
                (reduce (fn [acc v]
                          (when (:preferred v)
                            (reduced v)))
                        nil (list-apis
                              client
                              {:name    api
                               "fields" "items.discoveryRestUrl,items.name,items.version,items.preferred"}))
         _      (assert discovery-ref (str "api " api " not found"))
         api-discovery
                (get-discovery client discovery-ref)]
     (assoc client
       :api api
       :ops (prepare-resources
              api-discovery
              (or (:parameters api-discovery) [])
              (:resources api-discovery)))))
  ([config api version]
   (let [client (init-client config)
         discovery-ref
                (reduce (fn [acc v]
                          (when (= (:version v) version)
                            (reduced v)))
                        nil (list-apis
                              client
                              {:name    api
                               "fields" "items.discoveryRestUrl,items.name,items.version"}))
         _      (assert discovery-ref (str "api " api " version " version " not found"))
         api-discovery
                (get-discovery client discovery-ref)]
     (assoc client
       :api api
       :version version
       :ops (prepare-resources
              api-discovery
              (or (:parameters api-discovery) [])
              (:resources api-discovery))))))

(defn ops [client]
  (run!
    (fn [[id {:keys [description]}]]
      (println "* " id)
      (print description)
      (println \newline))
    (->> client :ops
         (sort-by key))))

(defn invoke [client {:keys [op] :as operation}]
  (let [opfn (-> client :ops (get op) :preform)]
    (assert opfn (str op
                      " is not implemented in "
                      (:api client)
                      (:version client)))
    (opfn client operation)))



(comment

  (def storage-client (dynamic-create-client {} "storage"))
  (deref (invoke storage-client {:op      "storage.buckets.list"
                                 :project "breezeehr.com:breeze-ehr"}))

  (ops storage-client))
