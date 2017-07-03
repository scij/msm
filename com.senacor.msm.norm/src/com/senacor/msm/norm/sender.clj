(ns com.senacor.msm.norm.sender
  (:require [com.senacor.msm.norm.norm-api :as norm]
            [com.senacor.msm.norm.control :as c]
            [com.senacor.msm.norm.util :as util]
            [clojure.core.async :refer [chan go-loop <! <!! >!! poll!]]
            [clojure.tools.logging :as log]))

(def ^:const buffer-size (* 1024 1024))

;;
;; Send messages across a norm stream
;;

(defn sender-handler
  [ctl-chan session event]
  (case (:event-type event)
    :tx-queue-vacancy (>!! ctl-chan :new-data)
    :tx-queue-empty (>!! ctl-chan :new-data)
    :local-sender-closed (>!! ctl-chan :local-sender-closed)
    :tx-flush-completed (>!! ctl-chan :tx-flush-completed)
    :tx-object-purged (>!! ctl-chan :tx-object-purged)
  ))

(defn- wait-for-event
  [chan event]
  (log/tracef "Waiting for %s" (str event))
  (loop [m (<!! chan)]
    (when (and m (not= m event))
      (recur (<!! chan))))
  (log/tracef "Received %s" (str event)))

(defn create-sender
  "Creates a message sender
  session NORM session used to communicate externally
  instance-id numeric identifier of the sender, used to distinguish
    senders in the NORM universe
  in-chan An async channel with byte-arrays of messages to be sent.
  max-msg-size in bytes, used to appropriately size the NORM buffers."
  [session instance-id in-chan max-msg-size]
  (norm/start-sender session instance-id buffer-size max-msg-size 64 16)
  (let [stream (norm/open-stream session buffer-size)
        ctl-chan (chan 10)]
    (c/register-sender session (partial sender-handler ctl-chan session))
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
                (poll! ctl-chan)
                (recur n-arr (count n-arr) 0))
              (do
                (log/tracef "wait free out buffer space, sent=%d")
                (<! ctl-chan)
                (log/trace "buffer space available")
                (recur b-arr (- b-len bytes-sent) (+ b-offs bytes-sent)))))
          )
        (do
          (log/trace "closing session")
          (norm/flush-stream stream true :passive)
          ;;(wait-for-event ctl-chan :tx-flush-completed)
          (norm/close-stream stream true)
          (wait-for-event ctl-chan :tx-object-purged)
          (norm/stop-sender session)
          (norm/destroy-session session)
          (c/unregister-sender session)
          (log/trace "session and stream closed"))
        ))))
