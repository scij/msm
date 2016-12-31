(ns com.senacor.msm.common.msg
  (:import (java.util Date)))

(defrecord Metadata
  [
   ^String label          ;; The message was received with this label
   ^Date   timestamp      ;; When was this message initially created
   ^String correlation-id ;; unchanged across the entire flow
   ])

(defrecord Message
  [
   ^Metadata metadata
   payload                ;; probably a Map
   ]
  )

(defn get-label
  "Returns the message label for the message."
  [^Message msg]
  (get-in msg :metadata :label))

(defn set-label
  "Returns a new message which is a copy of msg with the label replaced."
  [^Message msg ^String label]
  (->Message (->Metadata label
                         (get-in msg :metadata :timestamp)
                         (get-in msg :metadata :correlation-id))
             (:payload msg)))

(defn get-payload
  "Returns the message payload."
  [^Message msg]
  (:payload msg))

(defn set-payload
  "Returns a new message which is a cop of msg with the payload replaced."
  [^Message msg payload]
  (->Message (:metadata msg) payload))

(defn create-message
  "Convenience factory function to create a new message object."
  [^String label ^String correlation-id payload]
  (->Message (->Metadata label (Date.) correlation-id)
             payload))