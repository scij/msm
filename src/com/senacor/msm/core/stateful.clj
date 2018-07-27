(ns com.senacor.msm.core.stateful
  (:require [clojure.tools.logging :as log]
            [clojure.core.async :refer [alts! chan >!! <! go-loop timeout]]
            [com.senacor.msm.core.control :as control]
            [com.senacor.msm.core.util :as util]
            [com.senacor.msm.core.command :as command]
            [com.senacor.msm.core.norm-api :as norm]
            [com.senacor.msm.core.message :as message]
            [com.senacor.msm.core.raft-norm :as raft-norm]
            [com.senacor.msm.core.receiver :as receiver]))

(defn is-my-message
  [session subscription message]
  (message/label-match subscription message))

(defn stateful-session-handler
  [session subscription event-chan msg-chan]
  (let [cmd-chan-out (chan 5)
        cmd-chan-in (chan 5)
        a-leader? (atom false)]
    ; send status info to other receivers
    (norm/start-sender session (norm/get-local-node-id session) 2048 256 64 16)
    (command/command-sender session event-chan cmd-chan-out)
    (command/command-receiver session event-chan cmd-chan-in)
    (raft-norm/raft-state-machine subscription session a-leader? cmd-chan-in cmd-chan-out)
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
