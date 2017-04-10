(ns com.senacor.msm.norm.msg-test
  (:require [com.senacor.msm.norm.msg :refer :all]
            [clojure.test :refer :all]
            [bytebuffer.buff :as bb]
            [gloss.io :as gio])
  (:import (java.nio ByteBuffer)))

(defn print-bytes [bs]
  (doseq [b bs]
    (print b " "))
  (println))

(defn bytes-eq?
  [^bytes b1 ^bytes b2]
   (if (= (count b1) (count b2))
     (every? identity (map = b1 b2))
     false)
  )

(defn bb-eq?
  [^ByteBuffer b1 ^ByteBuffer b2]
  (bytes-eq? (.array b1) (.array b2)))

(deftest test-encode
  (testing "just encode"
    (let [fix (Message->bytebuffer (create-message "label" "uuid" "payload"))]
      (is fix)
      (is (= 26 (count (.array fix))))
      ))
  (testing "encode and inspect"
    (let [fix (Message->bytebuffer (create-message "label" "uuid" "payload"))]
      (is (= 26 (.remaining fix)))
      (is (= 77 (bb/take-byte fix)))
      (is (= 88 (bb/take-byte fix)))
      (is (= 1  (bb/take-byte fix)))
      (is (= 0  (bb/take-byte fix)))
      (is (= 5  (bb/take-byte fix)))
      (is (= (byte \l) (bb/take-byte fix)))
      (is (= (byte \a) (bb/take-byte fix)))
      (is (= (byte \b) (bb/take-byte fix)))
      (is (= (byte \e) (bb/take-byte fix)))
      (is (= (byte \l) (bb/take-byte fix)))
      (is (= 4 (bb/take-byte fix)))
      (is (= (byte \u) (bb/take-byte fix)))
      (is (= (byte \u) (bb/take-byte fix)))
      (is (= (byte \i) (bb/take-byte fix)))
      (is (= (byte \d) (bb/take-byte fix)))
      (is (= 7 (bb/take-int fix)))
      (is (= (byte \p) (bb/take-byte fix)))
      (is (= (byte \a) (bb/take-byte fix)))
      (is (= (byte \y) (bb/take-byte fix)))
      (is (= (byte \l) (bb/take-byte fix)))
      (is (= (byte \o) (bb/take-byte fix)))
      (is (= (byte \a) (bb/take-byte fix)))
      (is (= (byte \d) (bb/take-byte fix)))
      (is (= 0 (.remaining fix)))
      ))
  )

(deftest test-decoder
  (testing "parse message"
    (let [buf (doto (bb/byte-buffer 26)
                  (bb/put-byte 77)
                  (bb/put-byte 88)
                  (bb/put-byte 1)
                  (bb/put-byte 0)
                  (bb/put-byte 5)
                  (bb/put-byte (byte \h))
                  (bb/put-byte (byte \a))
                  (bb/put-byte (byte \l))
                  (bb/put-byte (byte \l))
                  (bb/put-byte (byte \o))
                  (bb/put-byte 4)
                  (bb/put-byte (byte \u))
                  (bb/put-byte (byte \u))
                  (bb/put-byte (byte \i))
                  (bb/put-byte (byte \d))
                  (bb/put-int 7)
                  (bb/put-byte (byte \p))
                  (bb/put-byte (byte \a))
                  (bb/put-byte (byte \y))
                  (bb/put-byte (byte \l))
                  (bb/put-byte (byte \o))
                  (bb/put-byte (byte \a))
                  (bb/put-byte (byte \d))
                  (.flip))]
      (is (= (String. (.array buf))
             (String. (.array (Message->bytebuffer (create-message "hallo" "uuid" "payload")))))))
    ))

