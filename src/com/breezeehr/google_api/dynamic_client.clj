(ns com.breezeehr.google-api.dynamic-client
  (:require [cheshire.core :as json]
            [com.breezeehr.google-api.java-auth
             :refer [add-auth init-client]]
            [com.breezeehr.google-api.boostrap
             :refer [list-apis get-discovery]]
            [clojure.alpha.spec :as s]
            [clojure.alpha.spec.gen :as gen]
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
                              request (assoc :body (cheshire.core/generate-string
                                                     (:request op))))
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
     (reset! lastmessage api-discovery )
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

(defn make-spec! [discovery path n {t :type :as schema}]
  (case t
    "object"
    (let [{:keys [properties]} schema]
      (if (empty? properties)
        (s/register
          (keyword path (name n))
          (s/resolve-spec 'clojure.core/map?))
        (do
          (doseq [[pname property] properties]
            (make-spec! discovery path pname property))
          (prn (->> (into
                      {}
                      (map (fn [[pname {t :type :as property}]]
                             [pname (keyword path (name pname))]
                             ))
                      properties)
                    list
                    (cons `s/schema)))
          (s/register
            (keyword path (name n))
            (s/resolve-spec
              (->> (into
                     {}
                     (map (fn [[pname {t :type :as property}]]
                            [pname (keyword path (name pname))]
                            ))
                     properties)
                   list
                   (cons `s/schema)))))))
    "string"
    (let [{:keys [properties]} schema]
      (s/register
        (keyword path (name n))
        (s/resolve-spec 'clojure.core/string?))
      )
    "boolean"
    (let [{:keys [properties]} schema]
      (s/register
        (keyword path (name n))
        (s/resolve-spec 'clojure.core/boolean?))
      )
    "integer"
    (let [{:keys [properties]} schema]
      (s/register
        (keyword path (name n))
        (s/resolve-spec 'clojure.core/integer?))
      )
    "array"
    (let [{:keys [items]} schema
          pname         (keyword (:$ref items))
          subschema (some-> discovery :schemas pname)]
      (make-spec! discovery path pname subschema)
      (s/register
        (keyword path (name n))
        (s/resolve-spec
          (->> (keyword path (name pname))
               list
               (cons 's/coll-of))))
      )))

(comment

  (def storage-client (dynamic-create-client {} "storage"))
  (deref (invoke storage-client {:op      "storage.buckets.list"
                                 :project "breezeehr.com:breeze-ehr"}))

  (ops storage-client))

(comment
  (def discovery @lastmessage)
  (-> discovery :schemas :Object)
  (def o (-> discovery :schemas :Object))
  (make-spec! discovery "blah" :Object o)
  )


(comment
  (s/register
    :blah/name2
    (s/resolve-spec 'clojure.core/string?))
  (s/register
    :blah/Object2
    (s/resolve-spec
      `(s/schema {:name :blah/name2})))
  (s/explain :blah/Object2 {:name "hi"} )
  ;-> Success!
  (s/explain :blah/Object2 {:name "hi"} {:closed #{:blah/Object2}})
  ;-> {:name "hi"} - failed: (subset? (set (keys %)) #{}) spec: :blah/Object2
(s/def :blah/name2 clojure.core/string?)
  (s/def :blah/Object2 (s/schema {:name :blah/name2}))
  ;https://github.com/clojure/spec-alpha2/blob/master/src/main/clojure/clojure/alpha/spec/impl.clj#L452
  )
