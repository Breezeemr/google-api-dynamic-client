(ns com.breezeehr.google-api.dynamic-client
  (:require [cheshire.core :as json]
            [com.breezeehr.google-api.java-auth
             :refer [add-auth init-client]]
            [com.breezeehr.google-api.boostrap
             :as bootstrap]
            [aleph.http :as http]
            [clj-wrap-indent.core :as wrap]
            [cemerick.url :as url]
            [clojure.string :as str]
            [manifold.deferred :as d]))


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
        (with-meta (meta m))))))

(defn param-pattern [param]
  (re-pattern (str "\\{\\+{0,1}" param"\\}")))

(defn make-path-fn [baseUrl path path-params parameterOrder id]
  (let [full-path path
        path-ops
        (transduce
          (filter (fn [param] (get path-params param)))
          (fn
            ([{:keys [path path-vec]}]
             (if path
               (conj path-vec (fn [^StringBuffer acc values]
                                (.append acc path)))
               path-vec))
            ([{:keys [path] :as acc} param]
             (assert (get path-params param) (str param (pr-str path-params)))
             (let [pattern (param-pattern param)
                   k (keyword param)
                   {validation-pattern "pattern" :as path-param} (get path-params param)
                   compiled-validation-pattern (when validation-pattern
                                          (re-pattern validation-pattern))
                   _
                   (assert (re-find pattern path) (str "\"" param "\" is not found in \"" full-path "\""))
                   [prefix left-over-path :as match] (clojure.string/split path pattern)]
               (assert (<= (count match) 2) (str "\"" param "\" is found more than once in \"" full-path "\""))
               (-> acc
                   (assoc :path left-over-path)
                   (update :path-vec into
                           [(fn [^StringBuffer acc values]
                              (.append acc prefix))
                            (fn [^StringBuffer acc values]
                              (let [v (k values)]
                                (assert v (str "your input " (pr-str values) " must contains key " k " for " id " path."))
                                (when compiled-validation-pattern
                                  (assert (re-matches compiled-validation-pattern v) (str "\"" v "\" did not match #\"" validation-pattern "\"")))
                                (.append acc v)))])))))
          {:path     path
           :path-vec [(fn [^StringBuffer acc values]
                        (.append acc baseUrl))]}
          parameterOrder)]
    ;(prn path path-ops)
    (fn [vals]
      (.toString (reduce
                   (fn [acc bfn]
                     (bfn acc vals))
                   (StringBuffer.)
                   path-ops)))))

(defn make-method [{:strs [baseUrl] :as api-discovery}
                   upper-parameters
                   {:strs [httpMethod parameterOrder path parameters request id]
                    :as   method-discovery}]
  (let [parameters (into upper-parameters parameters)
        init-map     {:method httpMethod
                      :as     :json}
        path-params  (into {} (filter (comp #(= % "path") #(get % "location") val)) parameters)
        path-fn      (make-path-fn baseUrl path path-params parameterOrder id)
        query-params (into {} (filter (comp #(= % "query") #(get % "location") val)) parameters)
        query-ks     (into [] (map key) query-params)
        key-sel-fn   (fast-select-keys query-ks)]
    ;(prn (keys method-discovery))
    [id {:id          id
         :description (get method-discovery "description")
         :parameters  (get method-discovery "parameters")
         :request
                      (fn [client op]
                        ;(clojure.pprint/pprint method-discovery)
                        ;(prn path path path-params)
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

(defn print-params [params]
  (doseq [[name {:strs [type required] opdesc "description"}] params]
    (println "      " name
             " type: "
             type
             (if required
               " required "
               " optional "))
    (wrap/println opdesc 80 14)))
(defn ops [client]
  (run!
    (fn [[id {:keys [description parameters] :as x}]]
      (println "* " id)
      (when-some [params (not-empty (filter (comp #{"path"} #(get % "location") second) parameters))]
        (println "   path-parameters:")
        (print-params params))
      (when-some [params (not-empty (filter (comp #{"query"} #(get % "location") second ) parameters))]
        (println "   query-parameters:")
        (print-params params))
      (println "   description:")
      (wrap/println description))
    (->> client :ops
         (sort-by key))))
(defn request [client {:keys [op] :as operation}]
  (let [reqfn (-> client :ops (get op) :request)]
    (assert reqfn (str op
                      " is not implemented in "
                      (:api client)
                      (:version client)))
    (reqfn client operation)))

;from https://github.com/cognitect-labs/aws-api/blob/v0.8.301/src/cognitect/aws/protocols/common.clj#L9
(def status-codes->anomalies
  {403 :cognitect.anomalies/forbidden
   404 :cognitect.anomalies/not-found
   503 :cognitect.anomalies/busy
   504 :cognitect.anomalies/unavailable})

(defn status-code->anomaly [code]
  (or (get status-codes->anomalies code)
      (if (<= 400 code 499)
        :cognitect.anomalies/incorrect
        :cognitect.anomalies/fault)))

(defn invoke [client operation]
  (let [req (request client operation)
        respd (http/request req)]
    (-> respd
        (d/chain'
          (fn [{:keys [body status] :as response}]
            (if (and (>= status 200)
                     (< status 300))
              (with-meta body
                         {:request  req
                          :response response})
              (with-meta {:cognitect.anomalies/category (status-code->anomaly status)}
                         {:request  req
                          :response response}))))
        (d/catch'
          (fn [e]
            (with-meta {:cognitect.anomalies/category :cognitect.anomalies/fault
                        :error e}
                       {:request  req}))))))



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
