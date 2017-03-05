(ns com.senacor.msm.zmq.net
  (:require [zeromq.zmq :as zmq]
            [clojure.edn :as edn]
            [com.senacor.msm.zmq.msg :as msg]
            [clojure.tools.logging :as log]
            [clojure.string :as str])
  (:import (java.util Date UUID)
           (java.util.regex Pattern)
           (com.senacor.msm.zmq.msg Message Metadata)
           (org.zeromq ZMQ$Socket ZMQ)
           (java.io Closeable)))

(declare init-zmq)
(declare get-label-static-prefix)

;;
;; Context management
;;

(def zmq-ctx (atom (zmq/context)))

(def get-instance-id
  "Returns the name of the processes MXBean which contains the
  process id and the hostname"
  (memoize (fn []
             (-> (java.lang.management.ManagementFactory/getRuntimeMXBean)
                 (.getName)))))

;;
;; Serializing data
;;

(def edn-readers {Message  msg/map->Message,
                  Metadata msg/map->Metadata})

;;
;; Socket type
;;
(defrecord Socket [
  ^ZMQ$Socket socket
  ^Pattern label]
  Closeable
  (close [this]
    (.close ^ZMQ$Socket (:socket this)))
  )

;;
;; Socket management
;;

(defn create-client-socket
  "Returns a new client socket.
  network-spec is the network address to bind to.
  label is the message label to send on all outgoing messages"
  [network-spec label]
  (->Socket
    (let [sock (zmq/socket @zmq-ctx :pub)]
      (when (str/starts-with? network-spec "epgm")
        (.setMulticastLoop sock true))
      (zmq/connect sock network-spec))
    (get-label-static-prefix label)))

(defn create-server-socket
  "Returns a new server socket.
  network-spec is the network address to bind to.
  label is the message label to subscribe to"
  [network-spec label]
  (let [socket (zmq/socket @zmq-ctx :sub)]
    (zmq/bind socket network-spec)
    (when (str/starts-with? network-spec "epgm")
      (.setMulticastLoop socket true))
    (zmq/subscribe socket label)
    (->Socket socket label)))

(defn close
  [^Socket sock]
  (zmq/close (:socket sock)))

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

(defmethod get-label-static-prefix nil
  [_]
  nil)

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
  ([^Socket sock label ^String payload]
   (doto (:socket sock)
     (zmq/send-str label zmq/send-more)
     (zmq/send-str "DATA" zmq/send-more)
     (zmq/send-str (msg/create-corr-id) zmq/send-more)
     (zmq/send-str (pr-str payload))))
  ([^Socket sock ^Message msg]
   (doto (:socket sock)
     (zmq/send-str (msg/get-label msg) zmq/send-more)
     (zmq/send-str "DATA" zmq/send-more)
     (zmq/send-str (msg/get-correlation-id msg) zmq/send-more)
     (zmq/send-str (pr-str (msg/get-payload msg)))))
  )

(defn- receive-str-all
  "Receive all frames of a message and return them
  as a collection of strings
  socket is the ZMQ network socket to receive from"
  [^ZMQ$Socket socket]
  (loop [acc []]
    (log/trace "receiving")
    (let [new-acc (conj acc (zmq/receive-str socket))]
      (log/trace "received")
      (if (zmq/receive-more? socket)
        (recur new-acc)
        new-acc))))

(defn ^Message zreceive
  "Receive a message from the specified ZMQ socket.
  Returns the message as a map containing metadata and payload.
  The function blocks until a suitable message\n  was received.
  socket ZMQ socket."
  [^Socket sock]
  (let [socket (:socket sock)
        label-re (:label sock)]
    (loop [frames (receive-str-all socket)]
      (log/trace "ZReceive: msg" (count frames) frames)
      (if (label-match? label-re (first frames))
        ;; Regular expression passt -> Nachricht verarbeiten
        (do
          (log/trace "ZReceive: label match")
          (msg/create-message
            (first frames)
            ;; skip the DATA qualifier
            (nth frames 2) ;; corr-id
            (edn/read-string {:readers edn-readers} (nth frames 3))))
        ;; Label passt nicht, nächste Nachricht versuchen)
        (do
          (log/trace "ZReceive: label no match")
          (recur (receive-str-all socket)))))))

(defn handle-messages
  "Der Name ist noch bescheuert. Das ist ein Kompaktserver, der eine
  Serviceadresse abonniert und die dort auflaufenden Nachrichten
  verarbeitet."
  [network-spec label handler-fn]
  (let [sock (create-server-socket network-spec label)]
    (loop [msg (zreceive sock)]
      (try
        (handler-fn msg)
        (catch Exception ex
          ;; report failure to sender
          (log/error ex "Exception handling message")))
      (recur (zreceive sock)))))