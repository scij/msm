(ns com.senacor.msm.zmq.stateful-server
  (:require [com.senacor.msm.zmq.msg :as msg]
            [com.senacor.msm.zmq.net :as net]
            [zeromq.zmq :as zmq]
            [overtone.at-at :refer [every mk-pool]]))


(def my-pool (mk-pool))

;;
;; Active/Passive
;;

(def is-active (atom false))

(defn active?
  "Check if the current process instance is active.
  It is the implementor's responsibility to ensure that
  only the active process instance has side effects"
  []
  @is-active)

(defn activate
  "Make the current process instance the active one.
  This shall only be done after consultation with the
  remaining processes on the same service."
  []
  (swap! is-active #(true)))

(defn deactivate []
  "Deactivate the current process. This function is
  usually invoked by self-checks in the process or
  when the DEAC message has been received"
  (swap! is-active #(false)))

;;
;; Processing status
;;

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

;;
;; Heartbeat and is-alive
;;

(defn- send-heartbeat
  [sock]
  (zmq/send-str sock (:label sock) zmq/send-more)
  (zmq/send-str sock "BEAT" zmq/send-more)
  (zmq/send-str sock (net/get-instance-id))
  )

(defn- launch-heartbeat
  [network-spec service-label]
  (let [heartbeat-sock (net/create-client-socket network-spec service-label)]
    (every 5000 (comp send-heartbeat heartbeat-sock) my-pool)))

(defn stageful-service
  [network-spec service-label request-handler]
  (let [data-sock (net/create-server-socket network-spec service-label)
        ctrl-sock (net/create-client-socket network-spec (str "$MSM." service-label))]
    (launch-heartbeat network-spec service-label)
    (loop [msg (net/zreceive data-sock)]
      (send-receipt ctrl-sock msg)
      (request-handler msg)
      (send-processed ctrl-sock msg)
      (recur (net/zreceive data-sock)))
    )
  )
