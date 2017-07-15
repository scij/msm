(ns com.senacor.msm.norm.sender-test
    (:require [clojure.test :refer :all]
              [clojure.core.async :refer [chan >!! close! timeout]]
              [com.senacor.msm.norm.sender :refer :all]))

(deftest test-wait-for-event
  (testing "gleich ein treffer"
    (let [event {:session 1 :event-type 1}
          test-c (chan 1)]
      (>!! test-c event)
      (close! test-c)
      (is (= event (wait-for-event test-c 1 1)))))
  (testing "gleich geschlossen"
    (let [test-c (chan 1)]
      (close! test-c)
      (is (nil? (wait-for-event test-c 1 1)))))
  (testing "nicht die passende session"
    (let [event {:session 1 :event-type 1}
          test-c (chan 1)]
      (>!! test-c event)
      (close! test-c)
      (is (nil? (wait-for-event test-c 2 1)))))
  (testing "nicht der passende event"
    (let [event {:session 1 :event-type 1}
          test-c (chan 1)]
      (>!! test-c event)
      (close! test-c)
      (is (nil? (wait-for-event test-c 1 2)))))
  (testing "beides passt nicht"
    (let [event {:session 1 :event-type 1}
          test-c (chan 1)]
      (>!! test-c event)
      (close! test-c)
      (is (nil? (wait-for-event test-c 2 2)))))
  )