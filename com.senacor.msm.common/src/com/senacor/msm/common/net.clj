(ns com.senacor.msm.common.net
  (:require [zeromq.zmq :as zmq]
            [clojure.edn :as edn]
            [com.senacor.msm.common.msg :as msg])
  (:import (java.util Date)
           (java.util.regex Pattern)
           (com.senacor.msm.common.msg Message Metadata)))

;;
;; Message label matching
;;

(defmulti label-match?
          (fn [match _] (class match)))

(defmethod label-match? String
  [match label]
  (= match label))

(defmethod label-match? Pattern
  [match label]
  (re-matches match label))

(defmethod label-match? nil
  [_ _]
  true)

;;
;; Sending an receiving
;;

(defn send
  "Send data to the specified socket.
  socket ZMQ socket.
  label String containing the message label for routing
  and filtering.
  payload Message payload to be transmitted. Should be a CLJ
  builtin type. Use of custom types and records will tie sender
  and receiver closer together than necessary."
  ([socket label payload]
   (zmq/send socket (pr-str (msg/map->Message {:metadata (msg/map->Metadata {:label  label
                                                                             :tsSent (Date.)}),
                                               :payload  payload}))))
  ([socket ^Message msg]
   (zmq/send socket (pr-str msg)))
  )



(defn receive
  "Receive a message from the specified ZMQ socket.
  Returns the message as a map containing metadata and payload.
  The function blocks until a suitable message\n  was received.
  socket ZMQ socket.
  label-re the message label or a RE pattern used to filter
  incoming messages. Messages that do not match the label string
  or RE are silently skipped. Use nil to receive all messages."
  [socket label-re]
  (loop [raw-msg (zmq/receive socket)]
    (let [msg (edn/read-string raw-msg)]
      (if (label-match? label-re (msg/get-label msg))
        msg
        (recur (zmq/receive socket))))))
