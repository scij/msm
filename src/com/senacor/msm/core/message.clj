(ns com.senacor.msm.core.message
  (:require [bytebuffer.buff :as bb]
            [clojure.string :as str]
            [clojure.core.async :refer [<! >! <!! >!! go-loop chan close!]]
            [clojure.tools.logging :as log]
            [com.senacor.msm.core.util :as util])
  (:import (java.nio Buffer ByteBuffer)
           (java.util UUID)
           (clojure.lang PersistentQueue)
           (java.util.regex Pattern)))

;;
;; Container for data across the NORM transport
;; with metadata and payload space and serialization and de-serialization
;;

(defrecord Message
  [^String label
   ^String correlation-id
   ^String payload])

(defn create-message
  ([^String label ^String corr-id ^String payload]
   (assert label "Label must not be nil")
   (assert (not (str/blank? label)) "Label must not be empty")
   (->Message label corr-id payload))
  ([^String label ^String payload]
   (->Message label (str (UUID/randomUUID)) payload)))

(defmulti label-match
          "checks if the msg's label matches the value given by match"
          (fn [match msg]
            (class match)))

(defmethod label-match Pattern
  [match msg]
  (re-matches match (:label msg)))

(defmethod label-match String
  [match msg]
  (= match (:label msg)))

(defmethod label-match nil
  [_ _]
  true)

;; Message Structure
;;
;; Numeric values are in network byte order
;;
;; magic-number 2 bytes "MX"
;; major version 1 byte
;; minor version 1 byte
;; header var part length short
;; payload length int
;; -- end of header fixed part (10 Bytes)
;; label-length 1 byte
;; label label-length bytes
;; corr-id-length 1 byte
;; corr-id corr-id-length bytes
;; -- end of header var part
;; payload payload-length bytes
;; -- end of message

(def ^:const version-major 1)
(def ^:const version-minor 0)

(def ^:const msg-prefix-m (byte \M))
(def ^:const msg-prefix-x (byte \X))

(def ^:const hdr-len 10)

(defn message-length
  [label corr-id payload]
  (+ hdr-len
     1 (count label)
     1 (count corr-id)
     (count payload)))

(defn ^Message fault-message
  [corr-id error-msg]
  (->Message "/sys/fault"
             (or corr-id "")
             error-msg))

(defn ^"[B" Message->bytes
  "Takes a message and returns a byte array with it's binary
  transport representation"
  [msg]
  (let [b-label (.getBytes (:label msg))
        b-corr-id (.getBytes (:correlation-id msg))
        b-payload (.getBytes (:payload msg))
        b-array (byte-array (message-length b-label b-corr-id b-payload))]
    (doto (ByteBuffer/wrap b-array)
      ;; Fixed header
      (bb/put-byte msg-prefix-m)
      (bb/put-byte msg-prefix-x)
      (bb/put-byte version-major)
      (bb/put-byte version-minor)
      (bb/put-short (+ 1 (count b-label) 1 (count b-corr-id)))
      (bb/put-int (count b-payload))
      ;; Header var part
      (bb/put-byte (count b-label))
      (.put b-label)
      (bb/put-byte (count b-corr-id))
      (.put b-corr-id)
      (.put b-payload)
      (.flip))
    b-array))

(declare parse-var-header)
(declare parse-payload)
(declare send-message)
(declare parse-fixed-header)
(declare start-state)

(defn parse-fixed-header
  "Parse the fixed part of the header from buf and return an
  updated state map"
  [b-arr]
  (let [buf (ByteBuffer/wrap b-arr)
        magic1 (bb/take-byte buf)
        magic2 (bb/take-byte buf)
        major-v (bb/take-byte buf)
        minor-v (bb/take-byte buf)
        hdr-var-length (bb/take-short buf)
        payload-length (bb/take-int buf)]
    (.mark buf)
    (log/tracef "hdr %d %d %d.%d hdr-len=%d payload-len=%d"
                magic1 magic2 major-v minor-v
                hdr-var-length payload-length)
    (if (or (not= magic1 msg-prefix-m) (not= magic2 msg-prefix-x))
      (do
        (log/errorf "Invalid magic msg prefix: %d %d" magic1 magic2)
        -1)
      (if (or (not= version-major major-v) (> minor-v version-minor))
        (do
          (log/errorf "Invalid msg version: %d %d" major-v minor-v)
          -1)
        (if (< hdr-var-length 4)
          (do
            (log/errorf "Invalid var header length %d" hdr-var-length)
            -1)
          (if (neg? payload-length)
            (do
              (log/errorf "Payload length must be >= 0: %d" payload-length)
              -1)
            [hdr-var-length payload-length]
            ))))))

(defn parse-var-header
  "Parse the var length metadata from the message header."
  [buf]
  (let [label (util/take-string buf)
        corr-id (util/take-string buf)]
    (if (= corr-id "")
      (log/errorf "Corr-id is empty, label is \"%s\"" label)
      (log/tracef "Var header is \"%s\" \"%s\"" label corr-id))
    [label corr-id]))

(defn parse-payload
  "Returns the payload string from buf. The expected payload length has been read
  from the fixed message header"
  [len buf]
  (util/take-string len buf))

(defn align-byte-arrays
  "Returns a transducer that takes incoming byte arrays
  and returns new byte arrays broken at message boundaries."
  []
  (fn [step]
    (let [state (volatile! {:bytes-required hdr-len,
                            :b-arr          (byte-array 0)})]
      (fn
         ([] step)
         ([result] (step result))
         ([result input]
          (let [arr (util/cat-byte-array (:b-arr @state) input)]
            (if (< (count arr) (:bytes-required @state))
              (do ; not enough bytes available
                (vreset! state {:bytes-required (- (:bytes-required @state) (count input)),
                                :b-arr          arr})
                result)
              (loop [n-arr arr
                     bytes-avail (count n-arr)] ; as much bytes as we need.
                (when (>= bytes-avail hdr-len)
                  (vreset! state {:bytes-required (reduce + hdr-len (parse-fixed-header n-arr)),
                                  :b-arr          n-arr}))
                (if (>= bytes-avail (:bytes-required @state))
                  (let [msg (util/byte-array-head n-arr (:bytes-required @state))
                        b-rest (util/byte-array-rest n-arr (:bytes-required @state))
                        n-bytes-avail (count b-rest)]
                    (vreset! state {:bytes-required hdr-len,
                                    :b-arr          b-rest})
                    (let [rtn (step result msg)]
                      (if (>= n-bytes-avail (:bytes-required @state))
                        (recur b-rest n-bytes-avail)
                        rtn)))
                  result))
           )))))))

(defn parse-message
  "takes a byte array and returns a Message object"
  [b-arr]
  (let [buf (ByteBuffer/wrap b-arr)
        hdr-buf (bb/slice-off buf hdr-len)
        [var-hdr-len payload-len] (parse-fixed-header b-arr)
        [label corr-id] (parse-var-header buf)
        payload (parse-payload payload-len buf)]
  (create-message label corr-id payload)))

(def message-rebuilder
  (comp (align-byte-arrays)
        (map parse-message)))