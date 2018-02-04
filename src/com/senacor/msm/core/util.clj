(ns com.senacor.msm.core.util
  (:require [clojure.string :as str]
            [clojure.java.jmx :as jmx]
            [clojure.core.async :refer [<!! >!!]]
            [clojure.tools.logging :as log]
            [bytebuffer.buff :as bb]
            [me.raynes.moments :as moments]
            [clojure.java.io :as io])
  (:import (java.nio Buffer ByteBuffer)
           (java.io OutputStream)
           (org.apache.logging.log4j ThreadContext)))

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
      (if  (and (< v 128 )(Character/isLetterOrDigit c)) (str c) "."))))



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
  [bytes]
  (let [parts (partition-all 16 bytes)]
    (for [part parts]
      [ (map hexl-hex part) (map hexl-char part) (map int part)])))

(defn hexlify-chars
  "Convert the bytes into a string of printable chars
   with . being used for unprintable chars"
  [bytes]
  (let [chars (mapcat second (hexlify bytes))]
    (str/join chars)))

(defn dump-bytes
  [b-arr]
  (if (nil? b-arr)
    (println "nil")
    (map #(let [[hexc charc intc] %]
            (println (partition-all 4 hexc)
                     (partition-all 4 (str/join charc))))
         (hexlify b-arr))))

(def dump-file (io/output-stream (io/file "target/dump.out")))

(defn dump-bytes-to-file
  [len b-arr]
  (.write dump-file (.getBytes "################################"))
  (.write dump-file b-arr 0 len)
  )


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

(defn byte-array-equal
  "Returns true if the content of two byte arrays matches"
  [ba1 ba2]
  (or
    (= ba1 ba2)
    (and
      (= (count ba1) (count ba2))
      (not (nil? ba1))
      (not (nil? ba2))
      (loop [i 0
             b1 (get ba1 i)
             b2 (get ba2 i)]
        (if (= b1 b2)
          (if (= i (count ba1))
            true
            (recur (inc i) (get ba1 i) (get ba2 i)))
          false)))))

(defn take-string
  "Reads a string from a byte buffer. The first variant reads
  a one byte length field from the buffer, the second takes
  an additional length argument and reads as many bytes from
  the buffer as specified in length."
  ([buf]
   (if (pos? (.remaining buf))
     (let [net-len (min (bb/take-byte buf) (.remaining buf))
           b (byte-array net-len)]
       (.get buf b 0 net-len)
       (String. b))
     ""))
  ([len buf]
   (let [net-len (min len (.remaining buf))
         b (byte-array net-len)]
     (.get buf b 0 net-len)
     (String. b))))

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

(defn wait-for-events
  "Pseudo-blocks waiting for one or more events.
  chan is the event channel where the events are received.
  session is the session to which the events related.
  event-types is a set of event-type keys the wait is expecting to receive.
  Returns the first event that matched the condition"
  [chan session event-types]
  (log/trace "Wait for events" event-types)
  (loop [m (<!! chan)]
    (if (and m
             (not (and (contains? event-types (:event-type m))
                       (= session (:session m)))))
      (recur (<!! chan))
      (do
        (log/trace "Wait ends with" (:event-type m))
        m))))

(defn init-logging
  [app-name]
  (System/setProperty "app" app-name)
  (log/infof "App startup %s" app-name))

(def sl-exec
  ;Scheduled executor to run keep alive and house keeping
  (moments/executor 2))

