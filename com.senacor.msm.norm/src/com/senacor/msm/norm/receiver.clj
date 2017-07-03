(ns com.senacor.msm.norm.receiver
  (:require [com.senacor.msm.norm.norm-api :as norm]
            [com.senacor.msm.norm.control :as c]
            [clojure.core.async :refer [>!! >! go-loop chan close!]]
            [com.senacor.msm.norm.util :as util]
            [clojure.tools.logging :as log])
  (:import (java.nio ByteBuffer)))

;;
;; Receive messages across a norm stream
;;

(def ^:const buf-size 1024)

(defn receiver-handler
  "Behandelt die NORM-Events, die für den Empfang von Nachrichten
  relevant sind. Diese Funktion wird im Controller registriert
  und von diesem aufgerufen, sobald ein RX-NORM-Event eintrifft."
  [session out-chan event]
  (case (:event-type event)
    :rx-object-updated
      (go-loop [buffer (byte-array buf-size)
             bytes-read (norm/read-stream (:object event) buffer buf-size)]
        (log/tracef "message received, len=%d" bytes-read)
        (when (> bytes-read 0)
          (let [nbuf (byte-array buf-size)]
            (>! out-chan (util/byte-array-head buffer bytes-read))
            (recur nbuf (norm/read-stream (:object event) nbuf buf-size)))))
    :rx-object-completed
      (do
        (norm/stop-receiver session)
        (c/unregister-receiver session)
        (norm/destroy-session session)
        (close! out-chan))
    :rx-object-aborted
      (do
        (norm/stop-receiver session)
        (c/unregister-receiver session)
        (norm/destroy-session session)
        (close! out-chan))
    :rx-object-new
      nil
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
  (norm/start-receiver session (* 10 buf-size))
  (c/register-receiver session (partial receiver-handler session chan))
  session)
