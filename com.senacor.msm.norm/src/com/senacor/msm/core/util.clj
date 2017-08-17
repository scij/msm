(ns com.senacor.msm.core.util
  (:require [clojure.string :as str])
  (:import (java.nio Buffer ByteBuffer)))

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

(defn dump-bytebuffer
  [msg buf]
  (print msg " ")
  (dump-bytes (.array buf))
  (println)
  (printf "limit %d\n" (.limit buf))
  (printf "remaining %d\n" (.remaining buf))
  (printf "position %d\n" (.position buf)))

(defn cat-byte-array
  "Returns a new byte array containing the data from b1 and be
  concatenated"
  [b1 b2]
  (let [result (byte-array (+ (count b1) (count b2)))]
    (System/arraycopy b1 0 result 0 (count b1))
    (System/arraycopy b2 0 result (count b1) (count b2))
    result))

(defn byte-array-rest
  "Returns a new byte array with a copy of everything
  in b-arr starting at pos. If pos is zero this function
  returns b-arr itself."
  [b-arr pos]
  (if (zero? pos)
    b-arr
    (let [result (byte-array (- (count b-arr) pos))]
      (System/arraycopy b-arr pos result 0 (- (count b-arr) pos))
      result)))

(defn byte-array-head
  "Returns a new byte array with a copy of everything
  in b-arr up to len. It len is equal to the size of
  b-arr, b-arr itself is returned."
  [b-arr len]
  (if (= len (count b-arr))
    b-arr
    (let [result (byte-array len)]
      (System/arraycopy b-arr 0 result 0 len)
      result)))

(defn parse-network-spec
  "Parses the network spec into its three elements
  1. network interface or network address of the interface
  2. multicast network address
  3. port number
  These elements are separated by a semicolon or a colon.
  Throws a number format exception when port is not numeric."
  [network-spec]
  (let [[interface mc-addr port] (str/split network-spec #"[;:]")]
    [interface
     mc-addr
     (Integer/parseInt port)
     ]))