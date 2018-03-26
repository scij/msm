(ns com.senacor.msm.core.stateless
  (:require [clojure.core.async :refer [go-loop close! poll! chan pipeline <! >! <!! >!! timeout]]
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

(def ^:const my-session "me")

(defn alive-sessions
  "Returns the map with all items removed where val->:expires is in the past"
  [sessions now]
  (log/trace "Alive sessions" sessions)
  (into (sorted-map)
        (filter (fn [rec]
                  (< now (:expires (second rec))))
                sessions)))

(defn session-is-alive
  "Returns a map of sessions with the given session having its lifetimer extended"
  [sessions remote-node-id subscription now]
  (log/tracef "Session is alive. Node=%s, Subscription=%s" remote-node-id subscription)
  ; todo gnothi seauton
  (merge sessions {remote-node-id {:expires (+ expiry-threshold now),
                                   :subscription subscription}}))

(defn number-of-sessions-alive
  "Returns the number of sessions currently alive"
  [_ sessions]
  (count sessions))

(defn find-my-index
  "Returns the index of the current session in the sessions table"
  [_ sessions]
  (log/trace "Find my index" sessions)
  (.indexOf (keys sessions) my-session))

(defn handle-receiver-status
  [session label cmd-chan-in a-session-receivers a-my-session-index a-receiver-count]
  (go-loop [cmd (<! cmd-chan-in)]
    (when cmd
      (log/trace "Command received" cmd)
      (let [remote-label (:subscription cmd)
            remote-node-id (norm/get-node-name (:node-id cmd))
            now (System/currentTimeMillis)]
        (when (= remote-label label)
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
  (log/trace "Enter housekeeping")
  (swap! a-session-receivers alive-sessions (System/currentTimeMillis))
  (log/trace "After alive-sessions" @a-session-receivers)
  (swap! a-receiver-count number-of-sessions-alive @a-session-receivers)
  (log/tracef "After count sessions %d" @a-receiver-count)
  (monitor/record-number-of-sl-receivers session @a-receiver-count)
  (swap! a-my-session-index find-my-index @a-session-receivers)
  (log/tracef "After my-index %d" @a-my-session-index)
  (log/trace "Exit housekeeping"))

(defn is-my-message
  "Returns true if the message sequence number matches the shard key
  and false otherwise. When the subscription has not been established yet
  false is returned and the sequence number is sent to the seq-no-chan.
  subscription is a regex
  seq-no-chan is an outbound channel where the message seq-nos are written
   as long as no subscription has been established.
  a-my-index and a-receiver-count are the ingredients to compute the sharding key.
  message is a msm message record\n"
  [seq-no-chan a-my-index a-receiver-count message]
  (log/tracef "is my message: my index %d receiver count %d seq-no %d"
              @a-my-index @a-receiver-count (:msg-seq-nbr message))
  (or
    (and (nil? @a-my-index)
         (>!! seq-no-chan (:msg-seq-nbr message))
         false)
    (and (some? @a-my-index)
         (= @a-my-index (mod (:msg-seq-nbr message) @a-receiver-count)))))

(defn prepare-to-join
  "Collects the message sequence numbers on seq-no-chan until it closes (by timeout),
  computes an estimate for the sequence number this session should join."
  [seq-no-chan a-join-seq-no]
  (log/info "Preparing to join - collecting data")
  (go-loop [initial-seq-no (<! seq-no-chan)
            prev-seq-no initial-seq-no
            seq-no (<! seq-no-chan)]
    (if seq-no
      (recur initial-seq-no seq-no (<! seq-no-chan))
      (do
        (assert (some? initial-seq-no) "Premature timeout on seq-no-chan")
        (let [join-seq-no (+ (* 2 (- prev-seq-no initial-seq-no)) initial-seq-no)]
          (reset! a-join-seq-no join-seq-no)
          (log/infof "Prepare to join complete. Join with %d" join-seq-no)))))
  a-join-seq-no)

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
        cmd-chan-in  (chan 64)
        a-session-receivers (atom (sorted-map my-session {:expires Long/MAX_VALUE,
                                                          :subscription subscription}))
        a-my-session-index (atom nil)
        a-my-session-last-msg (atom 0)
        a-receiver-count (atom 1)]
    ; send out status messages
    (norm/start-sender session (norm/get-local-node-id session) 2048 256 64 16)
    (command/command-sender session event-chan cmd-chan-out)
    (moments/schedule-every util/sl-exec alive-interval
                            (fn []
                              (log/trace "sending alive command msg")
                              (>!! cmd-chan-out (command/alive session subscription true @a-my-session-index @a-my-session-last-msg))))
    ; process status messages from other receivers
    (handle-receiver-status session subscription cmd-chan-in a-session-receivers a-my-session-index a-receiver-count)
    (moments/schedule-every util/sl-exec alive-interval
                            #(receiver-status-housekeeping session a-session-receivers a-receiver-count a-my-session-index))
    (command/command-receiver session event-chan cmd-chan-in)
    ; start receiving data. First in passive mode and once successfully joined actively
    (let [seq-no-chan  (timeout (* 2 alive-interval))
          a-join-seq-no (atom Long/MAX_VALUE)]
      ; start watching incoming messages and choose the sequence number to join at
      (prepare-to-join seq-no-chan a-join-seq-no)
      ; start processing incoming messages
      (receiver/create-receiver session event-chan msg-chan
                                (comp message/message-rebuilder
                                      (filter #(message/label-match subscription %))
                                      (filter #(> (:msg-seq-nbr %) @a-join-seq-no))
                                      (filter (partial is-my-message seq-no-chan
                                                       a-my-session-index a-receiver-count))))
  )))

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
