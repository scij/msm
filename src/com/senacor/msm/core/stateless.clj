(ns com.senacor.msm.core.stateless
  (:require [clojure.core.async :refer [go go-loop close! poll! put! chan pipeline <! >! <!! >!! timeout]]
            [me.raynes.moments :as moments]
            [com.senacor.msm.core.util :as util]
            [com.senacor.msm.core.control :as control]
            [com.senacor.msm.core.command :as command]
            [com.senacor.msm.core.message :as message]
            [com.senacor.msm.core.norm-api :as norm]
            [com.senacor.msm.core.receiver :as receiver]
            [clojure.tools.logging :as log]
            [com.senacor.msm.core.monitor :as monitor]))

(def ^:const alive-interval
  "Interval in ms to report that a receiver is alive"
  100)

(def ^:const expiry-threshold
  "Interval in ms after which a receiver should have reported alive and is considered
  dead otherwise. Must be greater than alive-interval"
  (* 2 alive-interval))

(defn alive-sessions
  "Returns the map with all items removed where val->:expires is in the past"
  [sessions now]
  (log/trace "Alive sessions at" now sessions)
  (into (sorted-map)
        (filter (fn [rec]
                  (< now (:expires (second rec))))
                sessions)))

(defn session-is-alive
  "Returns a map of sessions with the given session having its lifetimer extended"
  [sessions remote-node-id subscription now]
  (log/tracef "Session is alive. Node=%s, Subscription=%s" remote-node-id subscription)
  ; todo handle the case "new session" with a new map version bound to the join-seq-no
  (merge sessions {remote-node-id {:expires (+ expiry-threshold now),
                                   :subscription subscription}}))

(defn number-of-sessions-alive
  "Returns the number of sessions currently alive"
  [_ sessions]
  (count sessions))

(defn find-my-index
  "Returns the index of the current session in the sessions table"
  [sessions local-node-id]
  (log/trace "Find my index" sessions)
  (.indexOf (keys sessions) local-node-id))

(defn handle-receiver-status
  [session label cmd-chan-in a-session-receivers a-my-session-index a-receiver-count]
  (go-loop [cmd (<! cmd-chan-in)]
    (when cmd
      (let [remote-label (:subscription cmd)
            remote-node-id (norm/get-node-id (:node-id cmd))
            now (System/currentTimeMillis)]
        (when (and
                (not= remote-node-id (norm/get-local-node-id session))
                (= remote-label label))
          (swap! a-session-receivers session-is-alive remote-node-id remote-label now))
        )
      (recur (<! cmd-chan-in)))))

(defn receiver-status-housekeeping
  "Expire sessions that have not reported in with an alive message for twice
  the alive time interval.
  session the current NORM session.
  a-session-receivers the atom holding the active session map.
  a-receiver-count an atom holding the number of active receivers (including self).
  a-my-session-index an atom holding the index of the current session in session-receivers."
  [session a-session-receivers a-receiver-count a-my-session-index]
  (swap! a-session-receivers alive-sessions (System/currentTimeMillis))
  (swap! a-receiver-count number-of-sessions-alive @a-session-receivers)
  (monitor/record-number-of-sl-receivers session @a-receiver-count)
  (monitor/record-sl-receivers session @a-session-receivers)
  (reset! a-my-session-index (find-my-index @a-session-receivers (norm/get-local-node-id session)))
  (log/tracef "My session index %d" @a-my-session-index)
  ;(log/trace "Exit housekeeping")
  )

(defn is-my-message
  "Returns true if the message sequence number matches the shard key
  and false otherwise.
  a-my-index and a-receiver-count are the ingredients to compute the sharding key.
  message is a msm message record\n"
  [a-my-index a-receiver-count message]
  (log/tracef "is my message: my index %d receiver count %d seq-no %d"
              @a-my-index @a-receiver-count (:msg-seq-nbr message))
  (and (some? @a-my-index)
       (= @a-my-index (mod (:msg-seq-nbr message) @a-receiver-count))))

(defn join-filter
  "Returns a transducer on messages that first counts messages for two alive-intervals
  and estimates the message sequence number where this session will join processing
  (aka join-seq-no). No message with a sequence number below join-seq-no will pass this
  filter. All messages above this number will pass.
  cmd-chan-out is the channel where the join command will be published."
  [join-cmd-fn cmd-chan-out]
  (fn [step]
    ; todo handle case where no message has been received
    (let [join-seq-no (volatile! Long/MAX_VALUE)
          first-seq-no (volatile! 0)
          wait-end-ts (+ (util/now-ts) alive-interval alive-interval)]
      (fn
         ([] (step))
         ([result] (step result))
         ([result msg]
          (when (zero? @first-seq-no)
            (vreset! first-seq-no (:msg-seq-nbr msg))
            (log/tracef "First seq no %d" @first-seq-no))
          (when (and
                  (> (:receive-ts msg) wait-end-ts)
                  (= @join-seq-no Long/MAX_VALUE))
            (vreset! join-seq-no (+ (* 2 (- (:msg-seq-nbr msg) @first-seq-no)) @first-seq-no))
            (go
              (>! cmd-chan-out (join-cmd-fn @join-seq-no)))
            (log/tracef "Join seq no %d" @join-seq-no))
          (if (> (:msg-seq-nbr msg) @join-seq-no)
            (step result msg)
            result)
           )))))

(defn filter-fn-builder
  "Returns a transducer that transforms and filters the byte stream from NORM
  into a relevant (i.e. subscribed) messages.
  subscription A string or regex against which the message labels are matched.
  a-my-session-index This sessions index in the session table.
  a-receiver-count Number of receivers with the same subscription."
  [session subscription cmd-chan-out a-my-session-index a-receiver-count]
  (comp
    message/message-rebuilder
    (filter (partial message/label-match subscription))
    (join-filter (partial command/join session subscription) cmd-chan-out)
    (filter (partial is-my-message a-my-session-index a-receiver-count))))

(defn send-status-messages
  "Sends status messages every 'alive-interval'."
  [session subscription event-chan cmd-chan-out a-my-session-index a-my-session-last-msg]
  (norm/start-sender session (norm/get-local-node-id session) 2048 256 64 16)
  (command/command-sender session event-chan cmd-chan-out)
  (moments/schedule-every util/sl-exec alive-interval
                          (fn []
                            (log/trace "sending alive command msg")
                            (go (>! cmd-chan-out
                                    (command/alive session
                                                   subscription
                                                   true
                                                   @a-my-session-index
                                                   @a-my-session-last-msg)))
                            )))

(defn receive-status-messages
  "Listens for and provesses incoming status messages from other sessions."
  [session subscription event-chan a-session-receivers a-my-session-index a-receiver-count]
  ; process status messages from other receivers
  (let [cmd-chan-in (chan 64)]
    (handle-receiver-status session subscription cmd-chan-in a-session-receivers a-my-session-index a-receiver-count)
    (command/command-receiver session event-chan cmd-chan-in)
    (moments/schedule-every util/sl-exec alive-interval
                            (fn []
                              (receiver-status-housekeeping session
                                                            a-session-receivers
                                                            a-receiver-count
                                                            a-my-session-index)))
    ))

(defn receive-data
  "Receive data messages from the stream."
  [session subscription event-chan msg-chan cmd-chan-out a-my-session-index a-receiver-count]
  ; start receiving data. First in passive mode and once successfully joined actively
  (receiver/create-receiver session
                            event-chan
                            msg-chan
                            (filter-fn-builder session
                                               subscription
                                               cmd-chan-out
                                               a-my-session-index
                                               a-receiver-count)))

(defn stateless-session-handler
  "Starts sending out the alive-status messages and at the same time
  processes received inbound command messages
  session is the NORM session handle.
  subscription is a String or a Regex filtering the message label.
  event-chan is a mult channel with events from the instance control receiver.
  msg-chan is the channel where the accepted messages will be sent."
  ; todo buffer messages in case we need to reprocess them after another consumer failed
  [session subscription event-chan msg-chan]
  (let [cmd-chan-out (chan 16)
        a-session-receivers (atom (sorted-map (norm/get-local-node-id session) {:expires Long/MAX_VALUE,
                                                                                :subscription subscription}))
        a-my-session-index (atom nil)
        a-my-session-last-msg (atom 0)
        a-receiver-count (atom 1)]
    (send-status-messages session subscription event-chan cmd-chan-out a-my-session-index a-my-session-last-msg)
    (receive-status-messages session subscription event-chan a-session-receivers a-my-session-index a-receiver-count)
    (receive-data session subscription event-chan msg-chan cmd-chan-out a-my-session-index a-receiver-count)))

(defn create-session
  "Create a stateless session consuming matching messages in specified session
  instance is the NORM instance handle
  netspec is string specifying the session network address like eth0;239.192.0.1:7100
  subscription is a string or a regex to match the message label
  event-chan is a mult channel with events from the instances control receiver
  msg-chan is the channel where the session send all accepted messages.
  options is a map of network control options used to create the session"
  [instance netspec subscription event-chan msg-chan options]
  (let [[if-name network port] (util/parse-network-spec netspec)
        session (control/start-session instance if-name network port options)]
    (log/infof "Create stateless session on interface %s, address %s, port %d" if-name network port)
    (stateless-session-handler session subscription event-chan msg-chan)
    ))
