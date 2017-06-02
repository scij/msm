(ns com.senacor.msm.norm.control
  (:require [com.senacor.msm.norm.norm-api :as norm]
            [clojure.tools.logging :as log]
            [clojure.string :as str]))

;;
;; Implements the NORM event listener and dispatches
;; norm status updates to registered senders and receivers
;;

(def senders (atom {}))

(def receivers (atom {}))

(defn register-sender
  [session handler]
  (swap! senders assoc session handler))

(defn unregister-sender
  [session]
  (swap! senders dissoc session))

(defn register-receiver
  [session handler]
  (swap! receivers assoc session handler))

(defn unregister-receeiver
  [session]
  (swap! receivers dissoc session))

(defn- invoke-sender-callback
  [event]
  (when-let [f (get @senders (:session event))]
    (f event)))

(defn- invoke-receiver-callback
  [event]
  (when-let [f (get @receivers (:session event))]
    (f event)))

(defn- is-sender-event
  [event-type]
  (or
    (contains? #{:cc-active :cc-inactive} event-type)
    (str/starts-with? (str event-type) ":tx")
    ))

(defn- is-receiver-event
  [event-type]
  (str/starts-with? (str event-type) ":rx"))

(defn- event-loop
  [instance]
  (loop [event (norm/next-event instance)]
    (log/trace event)
    (cond
      (is-sender-event (:event-type event))
      (invoke-sender-callback event)
      (is-receiver-event (:event-type event))
      (invoke-receiver-callback event))
    (recur (norm/next-event instance))))