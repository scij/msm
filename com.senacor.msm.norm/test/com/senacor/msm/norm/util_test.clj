(ns com.senacor.msm.norm.util-test
  (:require [clojure.test :refer :all]
            [com.senacor.msm.norm.util :refer :all]))

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