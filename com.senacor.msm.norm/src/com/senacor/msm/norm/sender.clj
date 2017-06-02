(ns com.senacor.msm.norm.sender
  (:require [com.senacor.msm.norm.norm-api :as norm]
            [com.senacor.msm.norm.msg :as msg]
            [com.senacor.msm.norm.control :as c])
  (:import (com.sun.xml.internal.ws.client SenderException)
           (com.senacor.msm.norm.msg Message)))

;;
;; Send messages across a norm stream
;;

(defrecord Sender [session stream state buffer])

(defn sender-handler
  [sender session event]
  (case (:event-type event)
    :tx-queue-vacancy
    (norm/write-stream (:stream sender) (:buffer sender))
  )

(defn create-sender
  "Creates a message sender
  instance NORM instance handle
  address network address to send the messages to as a String
  port network destination port
  local-node-id an integer identifying this node uniquely"
  [instance address port local-node-id]
  ()
  (let [session (norm/create-session instance address port local-node-id)
        stream (norm/open-stream session 1024)
        sender (->Sender session stream (atom {}) (byte-array 1024))]
    (c/register-sender session (partial sender-handler sender))))

(defn send-message
  ([^Sender sender ^String label ^String payload]
   (msg/create-message label payload)
    )
  ([^Sender sender ^Message message]
    ))


