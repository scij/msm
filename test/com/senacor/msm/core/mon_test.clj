(ns com.senacor.msm.core.mon-test
  (:require [clojure.test :refer :all]
            [com.senacor.msm.core.monitor :refer :all]
            [clojure.java.jmx :as jmx]
            [com.senacor.msm.core.norm-api :as norm]))

(deftest test-register
  (testing "register/unregister"
    (let [node-id (System/currentTimeMillis)
          fix (register 1 "239.192.0.1" 7100 node-id)]
      (is (instance? clojure.java.jmx.Bean fix))
      (is (= (str "com.senacor.msm:type=Session,name=239.192.0.1/7100/" node-id) (get @session-names 1)))
      (unregister 1)
      (is (nil? (get @session-names 1)))
      )))

(deftest test-update-mon-status
  (testing "tx-rate-changed"
    (with-redefs-fn {#'norm/get-tx-rate (fn [_] 4711)}
      #(do
         (let [node-id (System/currentTimeMillis)
               fix (register 1 "239.192.0.1" 7100 node-id)]
           (update-mon-status {:session 1,
                               :event-type :tx-rate-changed})
           (is (= 4711 (jmx/read (get @session-names 1) :tx-rate)))
           (unregister 1)))))
  (testing "cc-active"
    (let [node-id (System/currentTimeMillis)
          fix (register 1 "239.192.0.1" 7100 node-id)]
      (update-mon-status {:session 1,
                          :event-type :cc-active})
      (is (jmx/read (get @session-names 1) :cc-active))
      (unregister 1)))
  (testing "cc-inactive"
    (let [node-id (System/currentTimeMillis)
          fix (register 1 "239.192.0.1" 7100 node-id)]
      (update-mon-status {:session 1,
                          :event-type :cc-inactive})
      (is (not (jmx/read (get @session-names 1) :cc-active)))
      (unregister 1)))
  (testing "grtt"
    (with-redefs-fn {#'norm/get-grtt-estimate (fn [_] 4711)}
      #(do
         (let [node-id (System/currentTimeMillis)
               fix (register 1 "239.192.0.1" 7100 node-id)]
           (update-mon-status {:session 1,
                          :event-type :grtt-updated})
           (is (= 4711 (jmx/read (get @session-names 1) :grtt)))
           (unregister 1)))))
  )

(deftest test-non-events
  (testing "bytes-sent"
    (let [node-id (System/currentTimeMillis)
          fix (register 1 "239.192.0.1" 7100 node-id)]
      (with-redefs-fn {#'jmx/read (fn [_ _] 100)}
        #(record-bytes-sent 1 123))
      (is (= 223 (jmx/read (get @session-names 1) :bytes-sent)))
      (unregister 1)))
  (testing "bytes-sent with nil"
    (let [node-id (System/currentTimeMillis)
          fix (register 1 "239.192.0.1" 7100 node-id)]
      (with-redefs-fn {#'jmx/read (fn [_ _] nil)}
        #(record-bytes-sent 1 123))
      (is (= 123 (jmx/read (get @session-names 1) :bytes-sent)))
      (unregister 1)))
  (testing "bytes-received"
    (let [node-id (System/currentTimeMillis)
          fix (register 1 "239.192.0.1" 7100 node-id)]
      (with-redefs-fn {#'jmx/read (fn [_ _] 100)}
        #(record-bytes-received 1 123))
      (is (= 223 (jmx/read (get @session-names 1) :bytes-received)))
      (unregister 1)))
  (testing "bytes-received with nil"
    (let [node-id (System/currentTimeMillis)
          fix (register 1 "239.192.0.1" 7100 node-id)]
      (with-redefs-fn {#'jmx/read (fn [_ _] nil)}
        #(record-bytes-received 1 123))
      (is (= 123 (jmx/read (get @session-names 1) :bytes-received)))
      (unregister 1)))
  )
