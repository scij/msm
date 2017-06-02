(ns com.senacor.msm.norm.receiver
  (:require [com.senacor.msm.norm.norm-api :as norm]
            [com.senacor.msm.norm.msg :as msg]
            [com.senacor.msm.norm.control :as c]
            [clojure.core.async :refer [>!! chan]])
  (:import (java.nio ByteBuffer)))

;;
;; Receive messages across a norm stream
;;

(def ^:const buf-size 1024)
(defrecord Receiver [session stream out-chan])

(defn receiver-handler
  "Behandelt die NORM-Events, die für den Empfang von Nachrichten
  relevant sind."
  [receiver session event]
  (case (:event-type event)
    :rx-object-updated
      (let [buffer (byte-array buf-size)
            bytes-read (norm/read-stream (:stream event) buffer buf-size)]
        (when (> 0 bytes-read)
          (>!! (:out-chan receiver) buffer)))))

(defn create-receiver
  [instance address port local-node-id chan]
  (let [session (norm/create-session instance address port local-node-id)
        stream (norm/open-stream session 1024)
        receiver (->Receiver session stream (chan))]
    (c/register-receiver session (partial receiver-handler receiver))
    (msg/bytes->Messages (:out-chan receiver))))
