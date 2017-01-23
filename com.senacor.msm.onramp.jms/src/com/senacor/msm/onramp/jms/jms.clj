(ns com.senacor.msm.onramp.jms.jms
  (:require [clojure.core.async :refer (>! go-loop)]
            [com.senacor.msm.common.msg :as msg])
  (:import (javax.jms Session ConnectionFactory Connection Queue JMSConsumer Message TextMessage)
           (com.tibco.tibjms TibjmsConnection TibjmsConnectionFactory)))

(def url "tcp://localhost:7778")

(def queue-name "MY.QUEUE.1")

(def ^ConnectionFactory conn-factory (TibjmsConnectionFactory. url))

(def ^Connection conn (.createConnection conn-factory))

(def ^Session session (.createSession conn false Session/AUTO_ACKNOWLEDGE))

(def ^Queue queue (.createQueue session queue-name))

(def ^JMSConsumer consumer (.createConsumer session queue))

(defn jms->chan
  "Subscribe to the JMS Queue and publish the messages to the
  async channel for further processing
  channel output channel"
  [jms-descr channel]
  (.start conn)
  (go-loop [jms-msg (.receive consumer)]
    (>! channel (msg/create-message queue-name
                                    (.getJMSCorrelationID jms-msg)
                                    (.getText ^TextMessage jms-msg)))
    (recur (.receive consumer))
    )
  channel
  )

