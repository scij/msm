(ns com.senacor.msm.core.stateful-test
  (:require [clojure.test :refer :all]
            [com.senacor.msm.core.stateful :refer :all]))

(deftest test-record-session-status
  (testing "empty status"
    (let [fixture {}]
      (is (= {"a" {"b" true}}
             (record-session-status fixture "a" "b" true 0))))
    )
  (testing "node known, unknown subscription"
    (let [fixture {"a" {"b" true}}]
      (is (= {"a" {"b" true, "c" false}}
             (record-session-status fixture "a" "c" false 0))))
    )
  (testing "node unknown, known subscription"
    (let [fixture {"a" {"b" true}}]
      (is (= {"a" {"b" true},
              "c" {"b" false}}
             (record-session-status fixture "c" "b" false 0)))))
  )

