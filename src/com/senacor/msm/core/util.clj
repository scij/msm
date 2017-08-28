(ns com.senacor.msm.core.util
  (:require [clojure.string :as str]
            [clojure.java.jmx :as jmx])
  (:import (java.nio Buffer ByteBuffer)))

;; Based on m0smith's code at https://gist.github.com/m0smith/1684476#file-hexlify-clj
(defprotocol Hexl
  (hexl-hex [val])
  (hexl-char [char]))

(extend-type Number
  Hexl
  (hexl-hex [i]
    (let [rtnval (Integer/toHexString (if (neg? i) (+ 256 i) i)) ]
      (if (< (count rtnval) 2) (str "0" rtnval) rtnval)))
  (hexl-char [b]
    (let [v (if (neg? b) (+ 256 b) b)
          c  (char v)]
      (if  (and (< v 128 )(Character/isLetterOrDigit c)) (.toString c) "."))))



(extend-type Character
  Hexl
  (hexl-hex [char]
    (hexl-hex (int (.charValue char))))
  (hexl-char [char]
    (hexl-char (int (.charValue char)))))

(defn hexlify
  "Perform similar to hexlify in emacs.  Accept a seq of bytes and
convert it into a seq of vectors.  The first element of the vector is a
seq of 16 strings for the HEX value of each byte.  The second element
of the vector is a seq of the printable representation of the byte and the
third elevment of thee vector is a seq of the integer value for each
byte.  Works for chars as well."
  ([bytes] (hexlify bytes 16))
  ([bytes size]
   (let [parts (partition-all size bytes)]
     (for [part parts]
       [ (map hexl-hex part) (map hexl-char part) (map int part)]))))

(defn hexlify-chars
  "Convert the bytes into a string of printable chars
   with . being used for unprintable chars"
  [bytes]
  (let [chars (mapcat second (hexlify bytes))]
    (apply str chars)))

(defn dump-bytes
  [b-arr]
  (if (nil? b-arr)
    (println "nil")
    (map #(let [[hexc charc intc] %]
            (println hexc (apply str charc)))
         (hexlify b-arr))))

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

(defn default-node-id
  "Returns the process id as a default value for the node id."
  []
  (Integer/parseInt (first (str/split (jmx/read "java.lang:type=Runtime" :Name) #"@"))))
