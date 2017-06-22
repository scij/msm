(ns com.senacor.msm.norm.sender
  (:require [com.senacor.msm.norm.norm-api :as norm]
            [com.senacor.msm.norm.control :as c]
            [com.senacor.msm.norm.util :as util]
            [clojure.core.async :refer [chan go-loop <! >!!]]))

(def ^:const buffer-size (* 1024 1024))

;;
;; Send messages across a norm stream
;;

(defrecord Sender [session stream in-chan])

(defn sender-handler
  [sender ctl-chan session event]
  (case (:event-type event)
    :tx-queue-vacancy (>!! ctl-chan :new-data)
    :tx-queue-empty (>!! ctl-chan :new-data)
  ))

(defn create-sender
  "Creates a message sender
  session NORM session used to communicate externally
  in-chan An async channel with byte-arrays of messages to be sent "
  [session instance-id in-chan max-msg-size]
  (norm/start-sender session instance-id buffer-size max-msg-size 64 16)
  (let [stream (norm/open-stream session buffer-size)
        ctl-chan (chan 10)
        sender (->Sender session stream in-chan)]
    (c/register-sender session (partial sender-handler sender ctl-chan))
    (go-loop [b-arr (<! in-chan)
              b-len (count b-arr)
              b-offs 0]
      (let [bytes-sent (norm/write-stream stream b-arr b-offs b-len)]
        (if (= bytes-sent b-len)
          (let [n-arr (<! in-chan)]
            (recur n-arr (count n-arr) 0))
          (do
            (<! ctl-chan)
            (recur b-arr (- b-len bytes-sent) (+ b-offs bytes-sent)))))
      )))
