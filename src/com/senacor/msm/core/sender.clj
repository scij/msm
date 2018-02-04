(ns com.senacor.msm.core.sender
  (:require [com.senacor.msm.core.norm-api :as norm]
            [com.senacor.msm.core.monitor :as mon]
            [com.senacor.msm.core.util :as util]
            [clojure.core.async :refer [chan close! go-loop sliding-buffer tap untap <! <!! >!! >! poll!]]
            [clojure.tools.logging :as log])
  )

(def ^:const buffer-size
  "Transmission buffer for the sender. Size is the same as in NormApp."
  (* 1024 1024))

;;
;; Send messages across a norm stream
;;

(defn stop-sender
  "Notify all receivers that this sender closes down. The sender will wait for
  confirmation by all receivers.
  session the NORM session that is closing down
  stream the NORM stream that is being closed
  event-chan delivers NORM notifications about the progress of the session shutdown"
  [session stream event-chan]
  (let [ec-tap (tap event-chan (chan 128))]
    (log/trace "closing sender")
    (norm/add-acking-node session norm/NORM_NODE_NONE)
    (norm/set-watermark session stream true)
    (util/wait-for-events ec-tap session #{:tx-watermark-completed})
    (norm/flush-stream stream true :passive)
    (norm/close-stream stream true)
    (norm/stop-sender session)
    (log/trace "stream closed" stream)
    (untap event-chan ec-tap)))

(defn create-sender
  "Creates a message sender
  session NORM session used to communicate externally
  instance-id numeric identifier of the sender, used to distinguish
    senders in the NORM universe
  event-chan is an inbound channel of NORM control events generated by
    the main event loop.
  in-chan An inbound channel with byte-arrays of messages to be sent.
  max-msg-size in bytes, used to appropriately size the NORM buffers."
  [session instance-id event-chan in-chan max-msg-size]
  (norm/set-congestion-control session true true)
  (norm/start-sender session instance-id buffer-size max-msg-size 64 16)
  (let [stream (norm/open-stream session buffer-size)
        ec-tap (chan (sliding-buffer 5) (filter #(contains? #{:tx-queue-vacancy :tx-queue-empty} (:event-type %))))]
    (tap event-chan ec-tap)
    (log/trace "Sender registered, starting loop")
    (go-loop [b-arr (<! in-chan)
              b-len (count b-arr)
              b-offs 0]
      (if b-arr
        (do
          (log/tracef "stream write len=%d, offs=%d" b-len b-offs)
          (let [bytes-sent (norm/write-stream stream b-arr b-offs b-len)]
            (mon/record-bytes-sent session bytes-sent)
            (if (= bytes-sent b-len)
              (do ; message completely sent.
                (norm/mark-eom stream)
                (let [n-arr (<! in-chan)]
                  (recur n-arr (count n-arr) 0)))
              (do ; not enough room for the entire message
                (log/tracef "wait free out buffer space, remaining=%d" (- b-len bytes-sent))
                (util/wait-for-events ec-tap session #{:tx-queue-empty :tx-queue-vacancy})
                (log/trace "buffer space available")
                (recur b-arr (- b-len bytes-sent) (+ b-offs bytes-sent)))))
          )
        (do
          (untap event-chan ec-tap)
          (stop-sender session stream event-chan)
          (log/trace "Exit sender loop"))
        ))))
