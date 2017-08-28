(ns com.senacor.msm.core.util-test
  (:require [clojure.test :refer :all]
            [com.senacor.msm.core.util :refer :all]))

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