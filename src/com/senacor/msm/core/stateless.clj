(ns com.senacor.msm.core.stateless
  (:require [clojure.core.async :refer [go-loop chan pipeline <! >! >!!]]
            [me.raynes.moments :as moments]
            [com.senacor.msm.core.util :as util]
            [com.senacor.msm.core.control :as control]
            [com.senacor.msm.core.command :as command]
            [com.senacor.msm.core.message :as message]
            [com.senacor.msm.core.norm-api :as norm]
            [com.senacor.msm.core.receiver :as receiver]
            [clojure.tools.logging :as log]
            [com.senacor.msm.core.monitor :as monitor]))

(def sl-exec
  ;Scheduled executor to run keep alive and house keeping
  (moments/executor 2))

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
  [session label cmd-chan-in session-receivers my-session-index receiver-count]
  (go-loop [cmd (<! cmd-chan-in)]
    (when cmd
      (log/trace "Command received" cmd)
      (let [remote-label (:subscription (command/parse-command (:cmd cmd)))
            remote-node-id (norm/get-node-name (:node-id cmd))
            now (System/currentTimeMillis)]
        (when (= remote-label label)
          (swap! session-receivers session-is-alive remote-node-id remote-label now))
        )
      (recur (<! cmd-chan-in)))))

(defn receiver-status-housekeeping
  [session session-receivers receiver-count my-session-index]
  (log/trace "Enter housekeeping")
  (swap! session-receivers alive-sessions (System/currentTimeMillis))
  (log/trace "After alive-sessions" @session-receivers)
  (swap! receiver-count number-of-sessions-alive @session-receivers)
  (log/tracef "After count sessions %d" @receiver-count)
  (monitor/record-number-of-sl-receivers session @receiver-count)
  (swap! my-session-index find-my-index @session-receivers)
  (log/tracef "After my-index %d" @my-session-index)
  (log/trace "Exit housekeeping"))

(defn is-my-message
  "Returns true if the message label matches the
  subscription and if the message correlation id matches the shard key
  and false otherwise.
  message is a msm message record
  subscription is a regex
  my-index and receiver-count are the ingredients to compute the sharding key."
  [subscription my-index receiver-count message]
  (and
    (message/label-match subscription message)
    (= @my-index (mod (.hashCode (:correlation-id message)) @receiver-count))))

(defn stateless-session-handler
  "Starts sending out the alive-status messages and at the same time
  processes received inbound command messages
  session is the NORM session handle.
  subscription is a String or a Regex filtering the message label.
  event-chan is a mult channel with events from the instance control receiver.
  msg-chan is the channel where the accepted messages will be sent."
  ; todo buffer messages in case we need to reprocess them after another consumer failed
  [session subscription event-chan msg-chan]
  (let [cmd-chan-out (chan 2)
        cmd-chan-in  (chan 5)
        session-receivers (atom (sorted-map my-session {:expires Long/MAX_VALUE,
                                                        :subscription subscription}))
        my-session-index (atom 0)
        receiver-count (atom 1)]
    (norm/start-sender session (norm/get-local-node-id session) 2048 256 64 16)
    (command/command-sender session event-chan cmd-chan-out)
    (moments/schedule-every sl-exec alive-interval 10
                            (fn []
                              (>!! cmd-chan-out (command/alive session subscription true))))
    (handle-receiver-status session subscription cmd-chan-in session-receivers my-session-index receiver-count)
    (moments/schedule-every sl-exec alive-interval
                            (partial receiver-status-housekeeping session session-receivers receiver-count my-session-index))
    (command/command-receiver session event-chan cmd-chan-in)
    (receiver/create-receiver session event-chan msg-chan
                              (comp message/message-rebuilder
                                    (filter (partial is-my-message subscription my-session-index receiver-count)))
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
