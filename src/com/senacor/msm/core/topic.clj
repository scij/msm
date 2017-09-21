(ns com.senacor.msm.core.topic
  (:require [clojure.core.async :refer [chan]]
            [com.senacor.msm.core.message :as message]
            [com.senacor.msm.core.norm-api :as norm]
            [com.senacor.msm.core.receiver :as receiver]
            [com.senacor.msm.core.util :as util]
            [com.senacor.msm.core.control :as control]
            [clojure.tools.logging :as log]))

(defn topic-session-handler
  "Starts sending out the alive-status messages and at the same time
  processes received inbound command messages
  session is the NORM session handle.
  subscription is a String or a Regex filtering the message label.
  event-chan is a mult channel with events from the instance control receiver.
  msg-chan is the channel where the accepted messages will be sent."
  [session subscription event-chan msg-chan]
  (let [bytes-chan (chan 5)]
    (receiver/create-receiver session event-chan bytes-chan)
    (message/bytes->Messages bytes-chan msg-chan)
    ))


(defn create-session
  "Create a topic session consuming matching messages in specified session
  instance is the NORM instance handle
  netspec is string specifying the session network address like eth0;239.192.0.1:7100
  subscription is a string or a regex to match the message label
  event-chan is a mult channel with events from the instances control receiver
  msg-chan is the channel where the session send all accepted messages.
  options is a map of network control options used to create the session"
  [instance netspec subscription event-chan msg-chan options]
  (let [[if-name network port] (util/parse-network-spec netspec)
        session (control/start-session instance if-name network port options)]
    (log/infof "Create topic session on interface %s, address %s, port %d" if-name network port)
    (topic-session-handler session subscription event-chan msg-chan)
    ))
