(ns com.senacor.msm.norm.util
  (:import (java.nio Buffer ByteBuffer)))

(defn dump-bytebuffer
  [msg buf]
  (print msg " ")
  (doseq [b (.array buf)]
    (print b " "))
  (println)
  (printf "limit %d\n" (.limit buf))
  (printf "remaining %d\n" (.remaining buf))
  (printf "position %d\n" (.position buf)))

(defn dump-bytes
  [b-arr]
  (doseq [b b-arr]
    (print b " "))
  (println))

(defn string-from-bytebuffer
  [buf]
  (let [b-arr (byte-array 32)]
    (.get buf b-arr)
    (String. b-arr)))

(defn cat-byte-array
  [b1 b2]
  (let [result (byte-array (+ (count b1) (count b2)))]
    (System/arraycopy b1 0 result 0 (count b1))
    (System/arraycopy b2 0 result (count b1) (count b2))
    result))

(defn byte-array-rest
  [b-arr pos]
  (let [result (byte-array (- (count b-arr) pos))]
    (System/arraycopy b-arr pos result 0 (- (count b-arr) pos))
    result))

(defn byte-array-head
  [b-arr len]
  (let [result (byte-array len)]
    (System/arraycopy b-arr 0 result 0 len)
    result))