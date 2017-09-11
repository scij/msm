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

(def ^:const ascii-soh (byte 1))
(def ^:const ascii-stx (byte 2))
(def ^:const ascii-etx (byte 3))
(def ^:const ascii-eot (byte 4))

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

(defn take-string
  "Reads a string of a given length from a byte buffer
  returning the string"
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

(declare parse-var-header)
(declare parse-payload)
(declare send-message)
(declare parse-fixed-header)
(declare start-state)

(defn skip-to-next-msg-prefix
  "Reads data from the buffer until the next valid
  message prefix is detected"
  [state buf out-chan]
  (log/trace "Skipping")
  (.mark buf)
  ;; todo besser mit den EOM anspringen und den vorliegenden Buffer verbrauchen
  (loop [step :scan]
    (if (.hasRemaining buf)
      (let [b (bb/take-byte buf)]
        (cond
          (and (= step :scan) (= b msg-prefix-m)) (recur :has-m)
          (and (= step :has-m) (= b msg-prefix-x)) (do
                                           (.reset buf)
                                           start-state)
          (and (= step :has-m) (not= b msg-prefix-x)) (do
                                              (.mark buf)
                                              (recur :scan))
          :else (do
                  (.mark buf)
                  (recur :scan))
          ))
      state)
    )
  )

(defn parse-fixed-header
  "Parse the fixed part of the header from buf and return an
  updated state map"
  [state buf _]
  (log/trace "parse fixed header" buf)
  (let [magic1 (bb/take-byte buf)
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
        {:valid? false,
         :complete? false,
         :parse-fn skip-to-next-msg-prefix})
      (if (or (not= version-major major-v) (< minor-v version-minor))
        (do
          (log/errorf "Invalid msg version: %d %d" major-v minor-v)
          {:valid? false,
           :complete? false,
           :parse-fn skip-to-next-msg-prefix})
        (if (< hdr-var-length 4)
          (do
            (log/errorf "Invalid var header length %d" hdr-var-length)
            {:valid? false,
             :complete? false,
             :parse-fn skip-to-next-msg-prefix})
          (if (neg? payload-length)
            (do
              (log/errorf "Payload length must be >= 0: %d" payload-length)
              {:valid? false,
               :complete? false,
               :parse-fn skip-to-next-msg-prefix})
            {:valid?         true,
             :hdr-var-length hdr-var-length,
             :payload-length payload-length,
             :bytes-required hdr-var-length,
             :parse-fn       parse-var-header,
             :complete?      false
             }
            ))))))

(defn parse-var-header
  "Parse the var length metadata from the message header."
  [state buf _]
  (let [label (take-string buf)
        corr-id (take-string buf)]
    (if (= corr-id "")
      (do
        (log/errorf "Corr-id is empty, label is %s" label)
        {:valid?    false,
         :complete? false,
         :parse-fn  skip-to-next-msg-prefix})
      (do
        (log/tracef "Var header is %s %s" label corr-id)
        {:label          label,
         :corr-id        corr-id,
         :bytes-required (:payload-length state),
         :parse-fn       parse-payload,
         :complete?      false
         }))))

(defn parse-payload
  "Reads as many payload bytes from the buf as specified in the
  payload element of the state and returns an updated state
  with the payload element set and the state re-initialized for
  the next message. The buffer is compacted to make room for
  another message"
  [state buf _]
  (let [result {:payload       (take-string (:payload-length state) buf)
                :parse-fn       send-message,
                :bytes-required 0,
                :complete?      true
               }]
    result))

(defn send-message
  [state buf out-chan]
  (>!! out-chan (->Message (:label state) (:corr-id state) (:payload state)))
  start-state)

(def start-state
  "Returns the initial state of the message processing state engine"
  {:parse-fn parse-fixed-header,
   :bytes-required hdr-len
   :complete? false})

(defn process-state
  [state buf out-chan]
  (log/tracef "invoke state func %s" (:parse-fn state))
  ((:parse-fn state) state buf out-chan))

(defn process-message
  [state byte-arr out-chan]
  (let [buf (ByteBuffer/wrap byte-arr)]
    (loop [st state]
      (if (>= (.remaining buf) (:bytes-required st))
        (recur (merge st (process-state st buf out-chan)))
        (merge st {:rest-arr (util/byte-array-rest (.array buf) (.position buf))}) )
      )
    )
  )

(defn bytes->Messages
  "Consumes a channel of byte arrays containing messages
  and message fragments and sends the messages to the
  outbound channel. This function is non-blocking and
  returns the out-chan."
  [in-chan out-chan]
  (let [state (atom start-state)]
    (go-loop [old-arr (.getBytes "")
              ^bytes new-arr (<! in-chan)]
      (log/trace "Message received:" (util/dump-bytes new-arr))
      (if new-arr
        (if (>= (+ (count new-arr) (count old-arr)) (:bytes-required @state))
          (do
            (log/trace "Processing message")
            (swap! state process-message (util/cat-byte-array old-arr new-arr) out-chan)
            (recur (:rest-arr @state) (<! in-chan)))
          (recur (util/cat-byte-array old-arr new-arr) (<! in-chan)))
        (close! out-chan))
      )
    )
  out-chan)