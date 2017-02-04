(ns com.senacor.msm.onramp.jms.jms
  (:require [clojure.core.async :refer (>! go-loop)]
            [com.senacor.msm.common.msg :as msg])
  (:import (javax.jms Session ConnectionFactory Connection Queue JMSConsumer Message TextMessage)
           (com.tibco.tibjms TibjmsConnection TibjmsConnectionFactory)))

(def conn-factory (atom nil))

(def conn (atom nil))

(defn init-tibjms
  [url]
  (swap! conn-factory (fn [_] (TibjmsConnectionFactory. url)))
  (swap! conn (fn [_] (.createConnection @conn-factory)))
  (.start conn))

(defn- create-consumer
  [queue-name ack-mode]
  (let [session (.createSession @conn false ack-mode)
        queue (.createQueue session queue-name)]
    (.createConsumer session queue)))

(defn jms->chan
  "Subscribe to the JMS Queue and publish the messages to the
  async channel for further processing
  channel output channel"
  [^JMSConsumer consumer channel]
  (go-loop [jms-msg (.receive consumer)]
    (>! channel (msg/create-message (.getJMSDestination jms-msg)
                                    (.getJMSCorrelationID jms-msg)
                                    (.getText ^TextMessage jms-msg)))
    (recur (.receive consumer)))
  channel
  )

