(ns com.senacor.msm.core.stateless
  (:require [clojure.core.async :refer [go chan <! >!!]]
            [me.raynes.moments :as moments]
            [com.senacor.msm.core.util :as util]
            [com.senacor.msm.core.control :as control]
            [com.senacor.msm.core.command :as command]
            [com.senacor.msm.core.message :as message]
            [com.senacor.msm.core.norm-api :as norm]
            [com.senacor.msm.core.receiver :as receiver]))

(def sl-exec (moments/executor 2))
(def ^:const expiry-threshold)

(defn handle-receiver-status
  [session label cmd-chan-in]
  (go
    (let [me (norm/get-node-name (norm/get-local-node-id session))
          session-receivers (atom (sorted-map {me {:expires nil}}))
          receiver-count (atom 1)
          my-index (atom 0)]
      (loop [cmd (command/parse-command (<! cmd-chan-in))]
        (when-let [{remote-node-id :node-id remote-label :subscription} cmd]
          (when (= remote-label label)
            (swap! session-receivers assoc remote-node-id
                   (+ expiry-threshold expiry-threshold (System/currentTimeMillis)))
            (swap! receiver-count (count session-receivers))
            (swap! my-index (.indexOf (keys session-receivers) session))
            ))
        (recur (<! cmd-chan-in))
        ;todo abgelaufene sessions entfernen
        )
      )))

(defn stateless-session-handler
  "Starts sending out the alive-status messages and at the same time
  processes received inbound command messages"
  [session label event-chan]
  (let [cmd-chan-out (chan 2)
        cmd-chan-in  (chan 5)]
    (moments/schedule-every sl-exec expiry-threshold 10
                            (fn []
                              (>!! cmd-chan-out (command/alive session label true))))
    (command/command-sender session event-chan cmd-chan-out)
    (handle-receiver-status session label cmd-chan-in)
    (command/command-receiver session event-chan cmd-chan-in)
  ))


(defn create-stateless
  [instance netspec label event-chan options]
  (let [[if-name network port] (util/parse-network-spec netspec)
        bytes-chan (chan 5)
        msg-chan (chan 5 (filter (partial message/label-match (re-pattern label))))
        session (control/start-session instance network port options)
        ]
    (when (if-name)
      (norm/set-multicast-interface session if-name))
    (receiver/create-receiver session event-chan bytes-chan)
    (message/bytes->Messages bytes-chan msg-chan)
    (stateless-session-handler session label event-chan)
    ))
