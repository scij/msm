(ns com.senacor.msm.zmq.async
  (:require [clojure.core.async :refer (go-loop <! >!)]
            [com.senacor.msm.zmq.net :as net]
            [zeromq.zmq :as zmq]
            [clojure.edn :as edn]))

;;
;; ZMQ async integration
;;

(defn zmq->chan
  "Receives messages from ZeroMQ and sends them to a clojure.core.async channel.
  Messages will be a map containing the message metadata and payload.
  The function will not return unless the receiving socket is closed.
  socket is the zmq socket where the messages are received.
  label is a regular expression to filter by message label. A string is fine too.
  channel is the channel to send the messages to."
  [socket channel]
  (go-loop [msg (net/zreceive socket)]
    (>! channel msg)
    (recur (net/zreceive socket)))
  channel)

(defn chan->zmq
  "Receives messages from channel, transforms them into ZeroMQ messages and sends
  them to the ZMQ network. Messages have to be maps containing metadata and payload.
  socket is the zmq socket whrere the messages are published.
  label is a string containing the message label subject.
  channel is the clojure.core.async input channel."
  [socket label channel]
  (go-loop [msg (<! channel)]
    (when msg
      (net/zsend socket label msg)
      (recur (<! channel))
      ))
  )
