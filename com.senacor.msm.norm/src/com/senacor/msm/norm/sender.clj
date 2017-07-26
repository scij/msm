(ns com.senacor.msm.norm.sender
  (:require [com.senacor.msm.norm.norm-api :as norm]
            [com.senacor.msm.norm.control :as c]
            [com.senacor.msm.norm.util :as util]
            [clojure.core.async :refer [chan go-loop tap untap <! <!! >!! >! poll!]]
            [clojure.tools.logging :as log]))

(def ^:const buffer-size (* 1024 1024))

;;
;; Send messages across a norm stream
;;

(defn sender-handler
  [session event-chan ctl-chan]
  (let [ec-tap (chan 5)]
    (tap event-chan ec-tap)
    (go-loop [event (<! ec-tap)]
      (log/trace "Event=" event)
      (if event
        (when (and (= session (:session event))
                   (or (= :tx-queue-vacancy (:event-type event))
                       (= :tx-queue-empty (:event-type event))))
          (>! ctl-chan :new-data)
          (recur (<! ec-tap)))
        (do
          (log/trace "Exit sender event loop")
          (untap event-chan ec-tap))))))

(defn wait-for-event
  "Does a blocking wait for event-type from session
  received from chan."
  [chan session event-type]
  (log/tracef "Waiting for %s" (str event-type))
  (loop [m (<!! chan)]
    (if (and m
               (not (and (= event-type (:event-type m))
                         (= session (:session m)))))
      (recur (<!! chan))
      m)))

(defn create-sender
  "Creates a message sender
  session NORM session used to communicate externally
  instance-id numeric identifier of the sender, used to distinguish
    senders in the NORM universe
  in-chan An async channel with byte-arrays of messages to be sent.
  max-msg-size in bytes, used to appropriately size the NORM buffers."
  [session instance-id event-chan in-chan max-msg-size]
  (norm/start-sender session instance-id buffer-size max-msg-size 64 16)
  (let [stream (norm/open-stream session buffer-size)
        ctl-chan (chan 10)]
    (sender-handler session event-chan ctl-chan)
    (log/trace "Sender registered, starting loop")
    (go-loop [b-arr (<! in-chan)
              b-len (count b-arr)
              b-offs 0]
      (if b-arr
        (do
          (log/tracef "stream write len=%d, offs=%d" b-len b-offs)
          (let [bytes-sent (norm/write-stream stream b-arr b-offs b-len)]
            (if (= bytes-sent b-len)
              (let [n-arr (<! in-chan)]
                (log/trace "ctl chan event=" (poll! ctl-chan))
                (recur n-arr (count n-arr) 0))
              (do
                (log/tracef "wait free out buffer space, sent=%d" (- b-len bytes-sent))
                (wait-for-event ctl-chan session :new-data)
                (log/trace "buffer space available")
                (recur b-arr (- b-len bytes-sent) (+ b-offs bytes-sent)))))
          )
        (let [ec-tap (tap event-chan (chan 5))]
          (log/trace "closing session")
          (norm/flush-stream stream true :passive)
          (norm/close-stream stream true)
          (Thread/sleep 1000)
          (norm/stop-sender session)
          (norm/destroy-session session)
          (log/trace "session and stream closed")
          (untap event-chan ec-tap))
        ))))
