(ns com.senacor.msm.norm.msg
  (:require [bytebuffer.buff :as bb]
            [clojure.string :as str]
            [gloss.core :as gc]
            [gloss.core.codecs :as gcc]
            [gloss.io :as gio]
            [manifold.stream :as s])
  (:import (java.nio ByteBuffer)
           (java.util UUID)))

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
   (->Message label (.toString (UUID/randomUUID)) payload)))

;; Message Structure
;;
;; Numeric values are in network byte order
;;
;; magic-number 2 bytes "MX"
;; major version 1 byte
;; minor version 1 byte
;; -- end of header fixed part (10 Bytes)
;; label-length 1 byte
;; label label-length bytes
;; corr-id-length 1 byte
;; corr-id corr-id-length bytes
;; -- end of header var part
;; payload payload-length bytes
;; -- end of message

(gc/defcodec msg-codec
             (gcc/ordered-map
               :magic1 :byte
               :magic2 :byte
               :major-version :byte
               :minor-version :byte
               :label (gc/finite-frame :byte (gc/string :utf-8))
               :corr-id (gc/finite-frame :byte (gc/string :utf-8))
               :payload (gc/finite-frame :int32 (gc/string :utf-8))
               )
             )

(defn Message->bytebuffer
  [msg]
  (gio/contiguous
    (gio/encode
      msg-codec
      {:magic1        77,
       :magic2        88,
       :major-version 1,
       :minor-version 0,
       :label         (:label msg),
       :corr-id       (:correlation-id msg),
       :payload       (:payload msg)
       })))

(defn frames->Message
  "Transforms decoded frames from gloss into a Message"
  [{:keys [magic1 magic2 major-version minor-version
           header-length label correlation-id payload]}]
  (when (or (not= magic1 77) (not= magic2 88))
    (throw (Exception. (format "Bad magic number %d %d" magic1 magic2))))
  (when (or (not= major-version 1) (< minor-version 0))
    (throw (Exception. (format "Incompatible Version %d.%d" major-version minor-version))))
  (->Message label correlation-id payload))

(defn decode-message
  "Consumes a manifold/stream of byte blocks and produces a stream of messages"
  [stream]
  (s/map frames->Message (gio/decode-stream stream msg-codec)))
