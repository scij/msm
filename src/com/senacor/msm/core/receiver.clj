(ns com.senacor.msm.core.receiver
  (:require [com.senacor.msm.core.norm-api :as norm]
            [com.senacor.msm.core.control :as c]
            [com.senacor.msm.core.monitor :as mon]
            [com.senacor.msm.core.util :as util]
            [clojure.core.async :refer [>!! >! <! tap untap go-loop chan close!]]
            [clojure.tools.logging :as log])
  (:import (java.nio ByteBuffer)
           (mil.navy.nrl.norm.enums NormSyncPolicy)))

;;
;; Receive messages across a norm stream
;;

(def ^:const buf-size 1024)

(defn receive-data
  "Receives as much data as there is available in the NORM network buffers.
  This function is called when the stream-update-event has been received.
  Since there is no way to find out how much data there is in the NORM network
  buffers we repeatedly read blocks until NORM indicates that all bytes have
  been read.
  out-chan is a channel where the buffer is sent
  event is the NORM event containing the input stream handle."
  [session stream out-chan]
  (go-loop [buffer (byte-array buf-size)
            bytes-read (norm/read-stream stream buffer buf-size)]
    (mon/record-bytes-received session bytes-read)
    (log/tracef "message received, len=%d" bytes-read)
    (when (pos? bytes-read)
      (>! out-chan (util/byte-array-head buffer bytes-read))
      (let [nbuf (byte-array buf-size)]
        (recur nbuf (norm/read-stream stream nbuf buf-size))))))

(defn close-receiver
  "Closes and gracefully stops the session. Releases all NORM resources and closes the
  out-channel. This function is called when the NORM stream has been closed
  on the sender side.
  session is the NORM session that is being closed.
  out-chan is the byte array channel associated with this session."
  [session out-chan]
  (log/trace "stopping session")
  (norm/stop-receiver session)
  (norm/destroy-session session)
  (mon/unregister session)
  (close! out-chan))

(defn stream-handler
  "Handles receiving from a particular stream."
  ; todo This will send byte arrays from different streams to the same chan
  ; solution could be to turn bytes->message into a transducer
  ; and pass the transducer down which would maintain decoupling
  ; of layers
  [session stream event-chan out-chan]
  (norm/seek-message-start stream)
  (let [ec-tap (chan 5)]
    (tap event-chan ec-tap)
    (go-loop [event (<! ec-tap)]
      (if event
        (if (and (= session (:session event))
                 (= stream (:object event)))
          (case (:event-type event-chan)
            :rx-object-updated
            (do
              (receive-data session stream out-chan)
              (recur (<! ec-tap)))
            (:rx-object-completed :rx-object-aborted)
            (log/info "Stream closed" (norm/event->str event))
            ;default
            (recur (<! ec-tap)))
          (recur (<! ec-tap)))
        (untap event-chan ec-tap)))))

(defn receiver-handler
  "Handles NORM-Events that relate to received messages. Hook this
  function to the event-chan of the control loop. It starts a go-loop
  and immediately returns.
  session is the session where we receive data.
  event-chan is the control loop event channel.
  out-chan is the channel where received data will be written. The channel
    will contain byte arrays. Due to datagram length restrictions a block sent
    my be received in multiple blocks."
  [session event-chan out-chan]
  (let [ec-tap (chan 5)]
    (tap event-chan ec-tap)
    (go-loop [event (<! ec-tap)]
      (if event
        (if (= session (:session event))
          (case (:event-type event)
            :remote-sender-new
            (do
              (log/info "New sender:" (norm/event->str event))
              (recur (<! ec-tap)))
            :remote-sender-active
            (do
              (log/info "Remote sender active" (norm/event->str event))
              (recur (<! ec-tap)))
            :remote-sender-inactive
            (do
              (log/info "Remote sender inactive" (norm/event->str event))
              (recur (<! ec-tap)))
            :rx-object-new
            (do
              (log/info "New stream opened:" (norm/event->str event))
              (stream-handler session (:object event) event-chan out-chan)
              ;; Maybe we should start a new go-loop to handle :rx-object-updated for
              ;; one particular stream.
              (recur (<! ec-tap)))
            :event-invalid ;; happens when the instance is unexpectedly shut down
            (close! out-chan)
            ;; default
            (recur (<! ec-tap)))
          (recur (<! ec-tap)))
        (do
          (log/trace "Exit receiver event loop")
          (untap event-chan ec-tap)))))
    session)

(defn create-receiver
  "Creates a receiver participant in a NORM communication. Returns
  a function that closes the receiver and the corresponding session.
  session is the NORM session created in control.
  event-chan is the stream of NORM events from control. Remember to
    tap the channel in case you have multiple receivers or senders
    in your application
  out-chan is the channel where the receiver posts the received
    messages. The channel contains byte arrays which may be broken
    at datagram boundaries and do not necessarily match the blocks
    sent"
  [session event-chan out-chan
   & {:keys [cache-limit socket-buffer silent]}]
  (when cache-limit
    (norm/set-rx-cache-limit session cache-limit))
  (when socket-buffer
    (norm/set-rx-socket-buffer session socket-buffer))
  (when silent
    (norm/set-silent-receiver session true silent))
  (norm/start-receiver session (* 10 buf-size))
  (norm/set-default-sync-policy session :stream)
  (receiver-handler session event-chan out-chan)
  (partial close-receiver session out-chan))
