(ns com.senacor.msm.core.command
  (:require [bytebuffer.buff :as bb]
            [clojure.tools.logging :as log]
            [clojure.core.async :refer [chan sliding-buffer tap untap go-loop <! >!]]
            [com.senacor.msm.core.norm-api :as norm]
            [com.senacor.msm.core.util :as util])
  (:import (java.nio ByteBuffer)))


(def ^:const CMD_ALIVE
  "A stateless or stateful process sends this command to inform it's peers
  that it is still alive and processing" 1)

(def ^:const CMD_JOIN
  "A stateless process sends this command to inform it's peers that
  it intends to join processing" 2)

(defn command-sender
  "Receive commands as byte array messages from cmd-chan and send them
  to all NORM receivers of this session. Use event-chan to receive
  confirmation that messages have been sent."
  [session event-chan cmd-chan]
  (log/trace "Starting command sender")
  (let [ec-tap (chan 3 (filter #(= (:event-type %) :tx-cmd-sent)))]
    (tap event-chan ec-tap)
    (go-loop [cmd (<! cmd-chan)]
      (when cmd
        (log/trace "Send command" cmd)
        (norm/send-command session cmd (count cmd) true)
        (util/wait-for-events ec-tap session #{:tx-cmd-sent})
        (recur (<! cmd-chan)))))
  cmd-chan)

(declare parse-command)

(defn command-receiver
  "Receive NORM commands and publish them as byte arrays on a channel.
  session is the NORM session on which the commands are broadcasted.
  event-chan is a mult of the control event channel where inbound commands
  are notified.
  cmd-chan is a channel of maps containing the :cmd, a byte array with the raw
  command and :node-id with the command sender node id"
  [session event-chan cmd-chan]
  (log/trace "Starting command receiver")
  (let [ec-tap (chan 20 (filter #(and (= :rx-object-cmd-new (:event-type %))
                                      (= session (:session %)))))]
    (tap event-chan ec-tap)
    (go-loop [event (<! ec-tap)]
      (let [cmd (merge (parse-command (norm/get-command (:node event)))
                       {:node-id (:node event)})]
        (log/trace "Received command" cmd)
        (>! cmd-chan cmd))
      (recur (<! ec-tap)))))

;; Command Structure
;;
;; Numeric values are in network byte order
;;
;; magic-number   2 bytes "CX"
;; major version  1 byte
;; minor version  1 byte
;; command type   1 byte
;; payload-length short
;; -- end of fixed part (6 bytes)
;; payload payload-length bytes
;; -- end of message

(defn put-fixed-header
  [buf command-type]
  (bb/with-buffer buf
                  (bb/put-byte (byte \C))
                  (bb/put-byte (byte \X))
                  (bb/put-byte 1)
                  (bb/put-byte 0)
                  (bb/put-byte command-type))
  buf)

(defn put-string
  [buf str]
  (bb/put-byte buf (count str))
  (.put buf (.getBytes str))
  buf)

(defn alive
  "Creates an ALIVE command message informing all participating SL or SF processes that the
  current node is alive and kicking. The command is returned as a byte array.
  session-id is the current session.
  subscription is the message label the consumer is listening to. It is either a String
  or the String representing the regex.
  active is true when this consumer assumes to be the active instance and false otherwise
  index of this session in the global session table.
  msg-seq-nbr is the last message sequence number this client has processed"
  [session-id subscription active my-session-index msg-seq-nbr]
  (let [result (bb/byte-buffer 256)]
    (put-fixed-header result CMD_ALIVE)
    (bb/put-byte result (if active 1 0))
    (bb/put-int result my-session-index)
    (bb/put-long result msg-seq-nbr)
    (put-string result (str subscription))
    (util/byte-array-head (.array result) (.position result))))

(defn join
  "Creates a JOIN command message informing all participating SL processes that the
  current node intends to join processing, starting with a given message sequence
  number. The message is returned as a byte array."
  [session-id subscription msg-seq-nbr]
  (let [result (bb/byte-buffer 256)]
    (put-fixed-header result CMD_JOIN)
    (bb/put-long result msg-seq-nbr)
    (put-string result (str subscription))
    (util/byte-array-head (.array result) (.position result))))

(defn parse-fixed-header
  [buf]
  (if (> (.remaining buf) 5)
    (let [magic1  (bb/take-byte buf)
          magic2  (bb/take-byte buf)
          major   (bb/take-byte buf)
          minor   (bb/take-byte buf)
          command (bb/take-byte buf)]
      (log/tracef "Parse command. Magic %c %c, Version %d.%d, command %d"
                  (char magic1) (char magic2) major minor command)
      (if (and (= magic1 (byte \C)) (= magic2 (byte \X)))
        (if (and (= 1 major) (>= 0 minor))
          command
          (do
            (log/errorf "Incompatible command version. Expected 1.0, received %d.%d" major minor)
            nil))
        (do
          (log/errorf "Unexpected command magic %c %c" (char magic1) (char magic2))
          nil)))
    (log/errorf "Command too short %s" (.array buf))))

(defn parse-alive-var-part
  [buf]
  (log/trace "Alive received")
  {:active (= 1 (bb/take-byte buf))
   :session-index (bb/take-int buf)
   :msg-seq-nbr (bb/take-long buf)
   :subscription (util/take-string buf)})

(defn parse-join-var-part
  [buf]
  (log/trace "Join received")
  {:msg-seq-nbr (bb/take-long buf),
  :subscription (util/take-string buf)})

(defn parse-command
  [b-arr]
  (let [buf (ByteBuffer/wrap b-arr)
        cmd (parse-fixed-header buf)]
    (merge {:cmd cmd}
            (cond
              (= cmd CMD_ALIVE) (parse-alive-var-part buf)
              (= cmd CMD_JOIN) (parse-join-var-part buf)
              :else
              (do
                (log/errorf "Unknown command type %d" cmd)
                nil)))))
