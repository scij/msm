(ns com.senacor.msm.zmq.msg-test
  (:require [clojure.test :refer :all]
            [com.senacor.msm.zmq.msg :refer :all]))

(deftest test-create-message
  (testing "create-message"
    (let [fixture (create-message "TEST.MSG" "12345" "Payload")]
      (is (= (get-label fixture) "TEST.MSG"))
      (is (= (get-correlation-id fixture) "12345"))
      (is (= (get-payload fixture) "Payload"))
      ))
  )

(deftest test-change-label
  (testing "change label"
    (let [msg (create-message "TEST.MSG" "12345" "Payload")
          fixture (set-label msg "TEST.MSG.1")]
      (is (not= fixture msg))
      (is (= (get-label fixture) "TEST.MSG.1"))
      (is (= (get-label msg) "TEST.MSG"))
      (is (= (get-correlation-id fixture) "12345"))
      (is (= (get-payload fixture) "Payload"))
      ))
  )

(deftest test-change-payload
  (testing "change payload"
    (let [msg (create-message "TEST.MSG" "12345" "Payload")
          fixture (set-payload msg "My Payload")]
      (is (not= fixture msg))
      (is (= (get-payload fixture) "My Payload"))
      (is (= (get-payload msg) "Payload"))
      (is (= (get-correlation-id fixture) "12345"))
      (is (= (get-label fixture) "TEST.MSG"))
      ))
  )
