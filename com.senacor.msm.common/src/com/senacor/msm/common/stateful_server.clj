(ns com.senacor.msm.common.stateful-server
  (:require [com.senacor.msm.common.msg :as msg]
            [com.senacor.msm.common.net :as net]
            [zeromq.zmq :as zmq]
            [overtone.at-at :refer [every mk-pool]]))

(defn- send-receipt
  [sock msg]
  (zmq/send-str sock (:label sock) zmq/send-more)
  (zmq/send-str sock "RCPT" zmq/send-more)
  (zmq/send-str sock (msg/get-correlation-id msg)))

(defn- send-processed
  [sock msg]
  (zmq/send-str sock (:label sock) zmq/send-more)
  (zmq/send-str sock "PRCD" zmq/send-more)
  (zmq/send-str sock (msg/get-correlation-id msg)))

(defn- send-heartbeat
  [sock]
  (zmq/send-str sock (:label sock) zmq/send-more)
  (zmq/send-str sock "BEAT" zmq/send-more)
  (zmq/send-str sock (net/get-instance-id))
  )

(def my-pool (mk-pool))
(defn- launch-heartbeat
  [network-spec service-label]
  (let [heartbeat-sock (net/create-client-socket network-spec service-label)]
    (every 5000 (comp send-heartbeat heartbeat-sock) my-pool)))

(defn stageful-service
  [network-spec service-label request-handler]
  (let [data-sock (net/create-server-socket network-spec service-label)
        ctrl-sock (net/create-client-socket network-spec service-label)]
    (launch-heartbeat network-spec service-label)
    (loop [msg (net/zreceive data-sock)]
      (send-receipt ctrl-sock msg)
      (request-handler msg)
      (send-processed ctrl-sock msg)
      (recur (net/zreceive data-sock)))
    )
  )
