(ns com.senacor.msm.core.stateful
  (:require [clojure.tools.logging :as log]
            [clojure.core.async :refer [chan >!! <! go-loop]]
            [com.senacor.msm.core.control :as control]
            [com.senacor.msm.core.util :as util]
            [com.senacor.msm.core.command :as command]
            [com.senacor.msm.core.norm-api :as norm]
            [me.raynes.moments :as moments]
            [com.senacor.msm.core.message :as message]
            [com.senacor.msm.core.receiver :as receiver]))

(def session-active
  "A map keyed by node-id and subscription with the active-state of
  the node as the value"
  (atom {}))

(def ^:const alive-interval 10)

(def a-last-message (atom 0))

(defn active?
  "Returns true if the given session is the current active one.
  Consumers are required to check this information before they create
  any externally visible side-effect while processing messages"
  [subscription]
  (get-in @session-active "localhost" subscription))

(defn my-status
  [session subscription]
  (command/alive session subscription (active? subscription) 0 @a-last-message))

(defn record-session-status
  [session-active remote-node-id remote-label remote-active now]
  (assoc-in session-active [remote-node-id remote-label] remote-active))

(defn handle-receiver-status
  "Records the alive status of a receiver"
  [session subscription cmd-chan-in session-active]
  (go-loop [cmd (<! cmd-chan-in)]
    (when cmd
      (log/trace "Command received" cmd)
      (let [remote-label (:subscription (command/parse-command (:cmd cmd)))
            remote-active (:active cmd)
            remote-node-id (norm/get-node-name (:node-id cmd))
            now (System/currentTimeMillis)]
        (when (= remote-label subscription)
          (swap! session-active record-session-status remote-node-id remote-label remote-active now))
        )
      (recur (<! cmd-chan-in)))))

(defn receiver-status-housekeeping
  "Checks if the active receiver has posted an alive-signal
  and initiates the election of a new active receiver if necessary"
  [session subscription session-active]
  )

(defn is-my-message
  [session subscription message]
  (and
    (active? session)
    (message/label-match subscription message)))

(defn stateful-session-handler
  [session subscription event-chan msg-chan]
  (let [cmd-chan-out (chan 5)
        cmd-chan-in (chan 5)]
    ; send status info to other receivers
    (norm/start-sender session (norm/get-local-node-id session) 2048 256 64 16)
    (command/command-sender session event-chan cmd-chan-out)
    (moments/schedule-every util/sl-exec alive-interval 10
                            (fn []
                              (>!! cmd-chan-out (my-status session subscription))))
    (log/fatal "Stateful sessions not yet implemented.")
    ; process status messages from other receivers
    (handle-receiver-status session subscription cmd-chan-in session-active)
    (moments/schedule-every util/sl-exec alive-interval
                            (partial receiver-status-housekeeping session subscription session-active))
    (command/command-receiver session event-chan cmd-chan-in)
    ; process incoming traffic
    (receiver/create-receiver session event-chan msg-chan
                              (comp message/message-rebuilder
                                    (filter (partial is-my-message session subscription))))
    ))

(defn create-session
  "Creates a new session for a stateful receiver.
  instance NORM instance handle
  netspec network address to receive the data
  subscription a regex or string to match in incoming messages
  event-chan channel of NORM events for flow control etc.
  msg-chan output channel where this session will put the received messages
  options a map of network control options used to create and parameterise the session"
  [instance netspec subscription event-chan msg-chan options]
  (let [[if-name network port] (util/parse-network-spec netspec)
        session (control/start-session instance if-name network port options)]
    (log/infof "Create stateful session on interface %s, address %s, port %d" if-name network port)
    (stateful-session-handler session subscription event-chan msg-chan)
    ))
