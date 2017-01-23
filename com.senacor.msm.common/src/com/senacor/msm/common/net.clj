(ns com.senacor.msm.common.net
  (:require [zeromq.zmq :as zmq]
            [clojure.edn :as edn]
            [com.senacor.msm.common.msg :as msg]
            [clojure.tools.logging :as log])
  (:import (java.util Date UUID)
           (java.util.regex Pattern)
           (com.senacor.msm.common.msg Message Metadata)
           (zmq ZMQ)))

;;
;; Context management
;;

(declare init-zmq)
(declare get-label-static-prefix)

(def zmq-ctx (atom (zmq/context)))

(def edn-readers {Message  msg/map->Message,
                  Metadata msg/map->Metadata})

;;
;; Socket management
;;

(defn create-client-socket
  "Returns a new client socket.
  network-spec is the network address to bind to."
  [network-spec]
  (doto (zmq/socket @zmq-ctx :pub)
    (zmq/connect network-spec)))

(defn create-server-socket
  "Returns a new server socket.
  network-spec is the network address to bind to.
  label is the message label to subscribe to"
  [network-spec label]
  (let [socket (zmq/socket @zmq-ctx :sub)]
    (zmq/bind socket network-spec)
    (when-not (nil? label)
      (zmq/subscribe socket (get-label-static-prefix label)))))

(defn close
  [sock]
  (zmq/close sock))

;;
;; Message label matching
;;

(defmulti label-match?
          (fn [match _] (class match)))

(defmethod label-match? String
  [match label]
  (log/trace "String match" match label)
  (= match label))

(defmethod label-match? Pattern
  [match label]
  (log/trace "Pattern match" match label)
  (re-matches match label))

(defmethod label-match? nil
  [_ _]
  (log/trace "Always true match")
  true)

(defmulti get-label-static-prefix
          (fn [l] (class l)))

(defmethod get-label-static-prefix String
  [label]
  label)

(defmethod get-label-static-prefix Pattern
  [label]
  (let [label-str (.pattern label)]
    (first (re-find #"^(/|\-|\w)+" label-str))))

;;
;; Sending an receiving
;; Messages are always sent as three frames:
;; * the message label
;; * the correlation id
;; * the payload
;;

(defn zsend
  "Send data to the specified socket.
  socket ZMQ socket.
  label String containing the message label for routing
  and filtering.
  payload Message payload to be transmitted. Should be a CLJ
  builtin type. Use of custom types and records will tie sender
  and receiver closer together than necessary."
  ([socket label ^String payload]
   (zmq/send-str socket label ZMQ/ZMQ_SNDMORE)
   (zmq/send-str socket (msg/create-corr-id) ZMQ/ZMQ_SNDMORE)
   (zmq/send-str socket (pr-str payload)))
  ([socket ^Message msg]
   (zmq/send-str socket (msg/get-label msg) ZMQ/ZMQ_SNDMORE)
   (zmq/send-str socket (msg/get-correlation-id msg) ZMQ/ZMQ_SNDMORE)
   (zmq/send-str socket (pr-str (msg/get-payload msg))))
  )

(defn ^Message zreceive
  "Receive a message from the specified ZMQ socket.
  Returns the message as a map containing metadata and payload.
  The function blocks until a suitable message\n  was received.
  socket ZMQ socket.
  label-re the message label or a RE pattern used to filter
  incoming messages. Messages that do not match the label string
  or RE are silently skipped. Use nil to receive all messages."
  [socket label-re]
  (loop [label (zmq/receive-str socket)]
    (log/trace "Received label" label)
    (if (label-match? label-re label)
      ;; Regular expression passt -> Nachricht verarbeiten
      (msg/create-message
        label
        (if (zmq/receive-more? socket)
          (zmq/receive-str socket)
          nil)
        (if (zmq/receive-more? socket)
          (edn/read-string {:readers edn-readers} (zmq/receive-str socket))
          nil))
      ;; Regular expression passt nicht, Nachricht verbrauchen aber nicht verarbeiten
      (while (zmq/receive-more? socket)
        (zmq/receive socket)))))

(defn handle-messages
  [network-spec label handler-fn]
  (let [sock (create-server-socket network-spec label)]
    (loop [msg (zreceive sock label)]
      (try
        (handler-fn msg)
        (catch Exception ex
          ;; request retransmission
          (log/error ex "Exception handling message")))
      (recur (zreceive sock label)))))