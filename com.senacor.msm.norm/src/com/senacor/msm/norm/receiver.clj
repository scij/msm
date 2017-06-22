(ns com.senacor.msm.norm.receiver
  (:require [com.senacor.msm.norm.norm-api :as norm]
            [com.senacor.msm.norm.control :as c]
            [clojure.core.async :refer [>!! chan close!]]
            [com.senacor.msm.norm.util :as util])
  (:import (java.nio ByteBuffer)))

;;
;; Receive messages across a norm stream
;;

(def ^:const buf-size 1024)
(defrecord Receiver [session stream out-chan])

(declare close-receiver)

(defn receiver-handler
  "Behandelt die NORM-Events, die für den Empfang von Nachrichten
  relevant sind. Diese Funktion wird im Controller registriert
  und von diesem aufgerufen, sobald ein RX-NORM-Event eintrifft."
  [receiver session event]
  (case (:event-type event)
    :rx-object-updated
      (loop [buffer (byte-array buf-size)
             bytes-read (norm/read-stream (:stream event) buffer buf-size)]
        (cond
          (= buf-size bytes-read) (let [nbuf (byte-array buf-size)]
                                    (>!! (:out-chan receiver) buffer)
                                    (recur nbuf
                                           (norm/read-stream (:stream event) nbuf buf-size)))
          (> 0 bytes-read) (let [nbuf (byte-array buf-size)]
                             (>!! (:out-chan receiver) (util/byte-array-head buffer buf-size))
                             (recur nbuf
                                    (norm/read-stream (:stream event) nbuf buf-size)))
          ))
    :rx-object-completed
      (do
        (c/unregister-receeiver session)
        (norm/stop-receiver session)
        (norm/destroy-session session)
        (close! (:chan receiver)))
    :rx-object-aborted
      (do
        (c/unregister-receeiver session)
        (norm/stop-receiver session)
        (norm/destroy-session session)
        (close! (:chan receiver)))
    ))

(defn create-receiver
  ;; todo doc comment
  [session chan
   & {:keys [cache-limit socket-buffer silent]}]
  (when cache-limit
    (norm/set-rx-cache-limit session cache-limit))
  (when socket-buffer
    (norm/set-rx-socket-buffer session socket-buffer))
  (when silent
    (norm/set-silent-receiver session true silent))
  (let [rcv-success (norm/start-receiver session (* 10 buf-size))
        stream (norm/open-stream session 1024)
        receiver (->Receiver session stream chan)]
    (assert rcv-success "Failed to start receiver")
    (c/register-receiver session (partial receiver-handler receiver))
    receiver))
