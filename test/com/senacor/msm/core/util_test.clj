(ns com.senacor.msm.core.util-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [chan >!! close!]]
            [com.senacor.msm.core.util :refer :all]
            [bytebuffer.buff :as bb])
  (:import (java.nio ByteBuffer)))

(deftest test-concat
  (testing "array cat"
    (is (= "hallo"
           (String. (cat-byte-array (.getBytes "") (.getBytes "hallo")))))
    (is (= "hallo"
           (String. (cat-byte-array (.getBytes "ha") (.getBytes "llo")))))
    (is (= "hallo"
           (String. (cat-byte-array (.getBytes "h") (.getBytes "allo")))))
    (is (= "hallo"
           (String. (cat-byte-array (.getBytes "hall") (.getBytes "o")))))
    ))

(deftest test-tail
  (testing "array substr tail"
    (is (= "hallo"
           (String. (byte-array-rest (.getBytes "hallo") 0))))
    (is (= "allo"
           (String. (byte-array-rest (.getBytes "hallo") 1))))
    (is (= "llo"
           (String. (byte-array-rest (.getBytes "hallo") 2))))
    (is (= "lo"
           (String. (byte-array-rest (.getBytes "hallo") 3))))
    (is (= "o"
           (String. (byte-array-rest (.getBytes "hallo") 4))))
    (is (= ""
           (String. (byte-array-rest (.getBytes "hallo") 5))))
    ))

(deftest test-head
  (testing "array substr head"
    (is (= ""
           (String. (byte-array-head (.getBytes "hallo") 0))))
    (is (= "h"
           (String. (byte-array-head (.getBytes "hallo") 1))))
    (is (= "ha"
           (String. (byte-array-head (.getBytes "hallo") 2))))
    (is (= "hal"
           (String. (byte-array-head (.getBytes "hallo") 3))))
    (is (= "hall"
           (String. (byte-array-head (.getBytes "hallo") 4))))
    (is (= "hallo"
           (String. (byte-array-head (.getBytes "hallo") 5))))
    ))

(deftest test-parse-network-spec
  (testing "all elements"
    (is (= ["en0" "239.192.0.1" 7100]
           (parse-network-spec "en0;239.192.0.1:7100"))))
  (testing "no interface provided"
    (is (= ["" "239.192.0.1" 7100]
           (parse-network-spec ";239.192.0.1:7100"))))
  (testing "network address as interface id"
    (is (= ["192.64.3.1" "239.192.0.1" 7100]
           (parse-network-spec "192.64.3.1;239.192.0.1:7100"))))
  (testing "named multicast network - unusual but possible"
    (is (= ["" "myhost.senacor.com" 7100]
           (parse-network-spec ";myhost.senacor.com:7100"))))
  (testing "named port"
    (is (thrown? NumberFormatException
                 (parse-network-spec ";239.192.0.1:font-service"))))
  (testing "missing port"
    (is (thrown? NumberFormatException
                 (parse-network-spec ";239.192.0.1:")))
    (is (thrown? NumberFormatException
                 (parse-network-spec ";239.192.0.1:")))
    )
  (testing "exchange ; and :"
    (is (= ["" "239.192.0.1" 7100]
           (parse-network-spec ":239.192.0.1;7100"))))
  )

(deftest test-dump-bytes
  (testing "simple text"
    (is = "")
    ))

(deftest test-wait-for-events
  (testing "gleich ein treffer"
    (let [event {:session 1 :event-type 1}
          test-c (chan 1)]
      (>!! test-c event)
      (close! test-c)
      (is (= event (wait-for-events test-c 1 #{1})))))
  (testing "gleich ein treffer aus vielen"
    (let [event {:session 1 :event-type 1}
          test-c (chan 1)]
      (>!! test-c event)
      (close! test-c)
      (is (= event (wait-for-events test-c 1 #{1 2})))))
  (testing "gleich geschlossen"
    (let [test-c (chan 1)]
      (close! test-c)
      (is (nil? (wait-for-events test-c 1 #{1})))))
  (testing "nicht die passende session"
    (let [event {:session 1 :event-type 1}
          test-c (chan 1)]
      (>!! test-c event)
      (close! test-c)
      (is (nil? (wait-for-events test-c 2 #{1})))))
  (testing "nicht der passende event"
    (let [event {:session 1 :event-type 1}
          test-c (chan 1)]
      (>!! test-c event)
      (close! test-c)
      (is (nil? (wait-for-events test-c 1 #{2})))))
  (testing "nicht der passende event aus mehreren"
    (let [event {:session 1 :event-type 1}
          test-c (chan 1)]
      (>!! test-c event)
      (close! test-c)
      (is (nil? (wait-for-events test-c 1 #{2 3})))))
  (testing "beides passt nicht"
    (let [event {:session 1 :event-type 1}
          test-c (chan 1)]
      (>!! test-c event)
      (close! test-c)
      (is (nil? (wait-for-events test-c 2 #{2})))))
  )

(deftest test-take-string
  (testing "take string"
    (let [buf (bb/byte-buffer 5)]
      (bb/put-byte buf 4)
      (bb/put-byte buf (byte \a))
      (bb/put-byte buf (byte \b))
      (bb/put-byte buf (byte \c))
      (bb/put-byte buf (byte \d))
      (.flip buf)
      (is (= (take-string buf) "abcd"))))
  (testing "take string length exceeded"
    (let [buf (bb/byte-buffer 5)]
      (bb/put-byte buf 5)
      (bb/put-byte buf (byte \a))
      (bb/put-byte buf (byte \b))
      (bb/put-byte buf (byte \c))
      (bb/put-byte buf (byte \d))
      (.flip buf)
      (is (= (take-string buf) "abcd"))))
  (testing "empty buffer"
    (let [buf (bb/byte-buffer 1)]
      (bb/put-byte buf 0)
      (.flip buf)
      (bb/take-byte buf)
      (is (= "" (take-string buf))))))

