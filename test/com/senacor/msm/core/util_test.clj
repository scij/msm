(ns com.senacor.msm.core.util-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [chan >!! close!]]
            [com.senacor.msm.core.util :refer :all]
            [bytebuffer.buff :as bb])
  (:import (java.nio ByteBuffer)))

(deftest test-concat
  (testing "array cat"
    (is (= "hallo"
           (String. ^bytes (cat-byte-array (.getBytes "") (.getBytes "hallo")))))
    (is (= "hallo"
           (String. ^bytes (cat-byte-array (.getBytes "ha") (.getBytes "llo")))))
    (is (= "hallo"
           (String. ^bytes (cat-byte-array (.getBytes "h") (.getBytes "allo")))))
    (is (= "hallo"
           (String. ^bytes (cat-byte-array (.getBytes "hall") (.getBytes "o")))))
    ))

(deftest test-tail
  (testing "array substr tail"
    (is (= "hallo"
           (String. ^bytes (byte-array-rest (.getBytes "hallo") 0))))
    (is (= "allo"
           (String. ^bytes (byte-array-rest (.getBytes "hallo") 1))))
    (is (= "llo"
           (String. ^bytes (byte-array-rest (.getBytes "hallo") 2))))
    (is (= "lo"
           (String. ^bytes (byte-array-rest (.getBytes "hallo") 3))))
    (is (= "o"
           (String. ^bytes (byte-array-rest (.getBytes "hallo") 4))))
    (is (= ""
           (String. ^bytes (byte-array-rest (.getBytes "hallo") 5))))
    ))

(deftest test-head
  (testing "array substr head"
    (is (= ""
           (String. ^bytes (byte-array-head (.getBytes "hallo") 0))))
    (is (= "h"
           (String. ^bytes (byte-array-head (.getBytes "hallo") 1))))
    (is (= "ha"
           (String. ^bytes (byte-array-head (.getBytes "hallo") 2))))
    (is (= "hal"
           (String. ^bytes (byte-array-head (.getBytes "hallo") 3))))
    (is (= "hall"
           (String. ^bytes (byte-array-head (.getBytes "hallo") 4))))
    (is (= "hallo"
           (String. ^bytes (byte-array-head (.getBytes "hallo") 5))))
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
      (.flip ^ByteBuffer buf)
      (is (= (take-string buf) "abcd"))))
  (testing "take string length exceeded"
    (let [buf (bb/byte-buffer 5)]
      (bb/put-byte buf 5)
      (bb/put-byte buf (byte \a))
      (bb/put-byte buf (byte \b))
      (bb/put-byte buf (byte \c))
      (bb/put-byte buf (byte \d))
      (.flip ^ByteBuffer buf)
      (is (= (take-string buf) "abcd"))))
  (testing "empty buffer"
    (let [buf (bb/byte-buffer 1)]
      (bb/put-byte buf 0)
      (.flip ^ByteBuffer buf)
      (bb/take-byte buf)
      (is (= "" (take-string buf))))))

(deftest test-byte-array-equal
  (testing "equal"
    (is (byte-array-equal (.getBytes "hallo") (.getBytes "hallo")))
    (is (byte-array-equal (.getBytes "h") (.getBytes "h")))
    (is (byte-array-equal (.getBytes "hällöchen") (.getBytes "hällöchen")))
    (is (byte-array-equal nil nil))
    (is (byte-array-equal (.getBytes "") (.getBytes "")))
    )
  (testing "not equal"
    (is (not (byte-array-equal (.getBytes "hallo") (.getBytes "hallö"))))
    (is (not (byte-array-equal (.getBytes "hi") (.getBytes "ho"))))
    (is (not (byte-array-equal (.getBytes "") (.getBytes "hallo"))))
    (is (not (byte-array-equal (.getBytes "hallo") (.getBytes ""))))
    (is (not (byte-array-equal (.getBytes "hallo") nil)))
    (is (not (byte-array-equal nil (.getBytes "hallo"))))
    (is (not (byte-array-equal (.getBytes "hallo") (.getBytes "hall"))))
    (is (not (byte-array-equal (.getBytes "") nil)))
    ))

(deftest test-get-local-process-id
  (is (< 0 (get-my-process-id)))
  (is (> Integer/MAX_VALUE (get-my-process-id))))

(deftest test-get-default-node-id
  (with-redefs-fn
    {#'get-interface-address (fn [_] (.getBytes "abcd")),
     #'get-my-process-id (fn [] 1234)}
    #(do
       (is (= 1667499218 (get-default-node-id "en3")))
       (is (= "99.100/1234" (printable-node-id 1667499218)))
      )))

(deftest test-get-interface-address
  (is (bytes? (get-interface-address "lo0")))
  (is (or
        (= 4 (count (get-interface-address "lo0")))
        (= 16 (count (get-interface-address "lo0"))))))