(ns com.senacor.msm.core.command
  (:require [bytebuffer.buff :as bb]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.core.async :refer [chan sliding-buffer tap untap go-loop <! >!]]
            [com.senacor.msm.core.norm-api :as norm]
            [com.senacor.msm.core.util :as util])
  (:import (java.nio Buffer ByteBuffer)))


(def ^:const CMD_SF_ALIVE
  "A stateful process sends this command to inform it's peers that it is still alive and processing" 1)
(def ^:const CMD_SL_PROCESSED
  "A stateless process sends this command to inform it's peers that it has processed a given message" 2)

(defn command-sender
  "Receive commands as byte array messages from cmd-chan and send them
  to all NORM receivers of this session. Use event-chan to receive
  confirmation that messages have been sent."
  [session event-chan cmd-chan]
  (let [ec-tap (chan (sliding-buffer 5))]
    (go-loop [cmd (<! cmd-chan)]
      (when cmd
        (tap event-chan ec-tap)
        (norm/send-command session cmd (count cmd) true)
        (util/wait-for-events ec-tap session #{:tx-cmd-sent})
        (untap event-chan ec-tap)
        (recur (<! cmd-chan)))))
  cmd-chan)

(defn command-receiver
  "Receive NORM commands and publish them as byte arrays on a channel.
  session is the NORM session on which the commands are broadcasted.
  event-chan is a mult of the control event channel where inbound commands
  are notified.
  cmd-chan is a channel of byte array to which the events are published."
  [session event-chan cmd-chan]
  (let [ec-tap (chan 20)
        node-id (norm/get-local-node-id session)]
    (tap event-chan ec-tap)
    (go-loop [event (<! ec-tap)]
      (when (and (= :rx-object-cmd-new (:event-type event))
                 (= session (:session event)))
        (>! cmd-chan (norm/get-command node-id))
        (recur (<! ec-tap))))))


;; Command Structure
;;
;; Numeric values are in network byte order
;;
;; magic-number   2 bytes "CX"
;; major version  1 byte
;; minor version  1 byte
;; node id        4 bytes
;; command type   1 byte
;; payload-length short
;; -- end of fixed part (6 bytes)
;; payload payload-length bytes
;; -- end of message

(defn length
  "Returns the length of the command array in bytes for a given command payload string"
  [& payload]
  (+ 9 (reduce + (map count payload)) (count payload)))

(defn put-fixed-header
  [buf node-id command-type]
  (bb/with-buffer buf
                  (bb/put-byte \C)
                  (bb/put-byte \X)
                  (bb/put-byte 1)
                  (bb/put-byte 0)
                  (bb/put-int node-id)
                  (bb/put-byte command-type))
  buf)

(defn put-string
  [buf str]
  (bb/put-byte buf (count str))
  (.put buf (.getBytes str))
  buf)

(defn alive
  "Creates an ALIVE command message informing all participating SF processes that the
  current node is alive and kicking
  node-id is the NORM node id of this node.
  subscription is the message label the consumer is listening to. It is either a String
  or the String representing the regex.
  active is true when this consumer assumes to be the active instance and false otherwise"
  [node-id subscription active]
  (let [result (bb/byte-buffer (length subscription))]
    (put-fixed-header result node-id CMD_SF_ALIVE)
    (bb/put-byte result (if active 1 0))
    (put-string result (str subscription))))

(defn processed [node-id uuid]
  (let [result (bb/byte-buffer (length uuid))]
    (put-fixed-header result node-id CMD_SL_PROCESSED)
    (put-string result (str uuid))))

(defn parse-fixed-header
  [buf]
  (let [magic1  (bb/take-byte buf)
        magic2  (bb/take-byte buf)
        major   (bb/take-byte buf)
        minor   (bb/take-byte buf)
        node-id (bb/take-int buf)
        command (bb/take-byte (buf))]
    (log/tracef "Parse command. Magic %c %c, Version %d.%d, src node %d command %d")
    (when (and (= magic1 \C) (= magic2 \X))
      (log/errorf "Unexpected control magic %c &c" magic1 magic2))
    (when (and (= 1 major) (>= 0 minor))
      (log/errorf "Incompatible command version. Expected 1.0, received %d.%d" major minor))
    ))