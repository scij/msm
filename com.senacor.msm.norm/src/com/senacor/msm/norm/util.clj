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
  (if b-arr
    (with-out-str
      (doseq [b b-arr]
        (if-let [c (char-escape-string (char b))]
          (printf "%3s " c)
          (if (Character/isLetterOrDigit (char b))
            (printf "  %c " (char b))
            (printf "%03o " b)))))
    "nil"))

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
  (if (zero? pos)
    b-arr
    (let [result (byte-array (- (count b-arr) pos))]
      (System/arraycopy b-arr pos result 0 (- (count b-arr) pos))
      result)))

(defn byte-array-head
  [b-arr len]
  (if (= len (count b-arr))
    b-arr
    (let [result (byte-array len)]
      (System/arraycopy b-arr 0 result 0 len)
      result)))