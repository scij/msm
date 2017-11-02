(ns com.senacor.msm.core.receiver
  (:require [com.senacor.msm.core.norm-api :as norm]
            [com.senacor.msm.core.control :as c]
            [com.senacor.msm.core.monitor :as mon]
            [com.senacor.msm.core.util :as util]
            [clojure.core.async :refer [>!! >! <! tap admix unmix mix untap go go-loop chan close!]]
            [clojure.tools.logging :as log])
  (:import (java.nio ByteBuffer)
           (mil.navy.nrl.norm.enums NormSyncPolicy)))

;;
;; Receive messages across a norm stream
;;

(def ^:const buf-size (* 1024 1024))

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
  "Handles receiving from a particular stream. It scans the event chan
  for rx-object-updated events carrying the correct session and stream id,
  reads byte arrays and sends them to stream-chan
  session NORM session
  stream NORM stream being handled
  event-chan NORM events
  out-mix is the chan where all stream channels are mixed.
  stream-chan the stream output channel."
  [session stream event-chan out-mix stream-chan]
  (log/debug "Enter stream handler" session stream)
  (norm/seek-message-start stream)
  (let [stream-events (chan 5 (filter #(and (= session (:session %))
                                            (= stream (:object %)))))]
    (tap event-chan stream-events)
    ; fake one event because we may have missed the first one already
    (>!! stream-events {:event-type :rx-object-updated,
                        :session session,
                        :object stream})
    (go-loop [event (<! stream-events)]
      (log/trace "next event" (norm/event->str event-chan))
      (if event
        (case (:event-type event)
          :rx-object-updated
          (do
            (log/trace "Stream new data:" (norm/event->str event))
            (receive-data session stream stream-chan)
            (recur (<! stream-events)))
          (:rx-object-completed :rx-object-aborted)
          (do
            (log/info "Stream closed:" (norm/event->str event))
            (unmix out-mix stream-chan))
          ;default
          (recur (<! stream-events)))
        (untap event-chan stream-events)))))

(defn receiver-handler
  "Handles NORM-Events that relate to received messages. Hook this
  function to the event-chan of the control loop. It starts a go-loop
  and immediately returns.
  session is the session where we receive data.
  event-chan is the control loop event channel.
  out-chan is the channel where received data will be written. The channel
    will contain byte arrays. Due to datagram length restrictions a block sent
    my be received in multiple blocks.
  message-builder is a transducer that builds messages from a sequence of
    byte arrays."
  [session event-chan out-chan message-builder]
  (let [ec-tap (chan 5)
        out-mix (mix out-chan)]
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
            (let [stream-chan (chan 5 message-builder)]
              (admix out-mix stream-chan)
              (log/info "Stream opened:" (norm/event->str event))
              (stream-handler session (:object event) event-chan out-mix stream-chan)
              (recur (<! ec-tap)))
            :event-invalid                                ;; happens when the instance is unexpectedly shut down
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
    sent
  message-builder is a transducer that combines byte-arrays to messages."
  [session event-chan out-chan message-builder
   & {:keys [cache-limit socket-buffer silent]}]
  (when cache-limit
    (norm/set-rx-cache-limit session cache-limit))
  (when socket-buffer
    (norm/set-rx-socket-buffer session socket-buffer))
  (when silent
    (norm/set-silent-receiver session true silent))
  (norm/start-receiver session (* 10 buf-size))
  (norm/set-default-sync-policy session :stream)
  (receiver-handler session event-chan out-chan message-builder)
  (partial close-receiver session out-chan))
