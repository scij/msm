(ns com.senacor.msm.core.monitor
  (:require [com.senacor.msm.core.norm-api :as norm]
            [clojure.core.async :refer [chan go-loop tap <! untap]]
            [clojure.java.jmx :as jmx]
            [clojure.tools.logging :as log]))

;; Maps sessions to JMX bean names.
(def session-names (atom {}))

; todo implement minute and second-based sampling
; for each session poll the tx-rate every second and update the bean
; use me.raynes/moments for scheduling: https://github.com/Raynes/moments
; use net.mikera/timeline to store and maintain the samples: https://github.com/mikera/timeline

(defn- jmx-write
  [mbean prop value]
  (log/tracef "jmx write %s = %s" (str prop) (str value))
  (jmx/write! mbean prop value))

(defn update-mon-status
  [event]
  (let [session (:session event)
        mbean (get @session-names session)]
    (case (:event-type event)
      :tx-rate-changed (jmx-write mbean :tx-rate (norm/get-tx-rate session))
      :cc-active (jmx-write mbean :cc-active true)
      :cc-inactive (jmx-write mbean :cc-active false)
      :grtt-updated (jmx-write mbean :grtt (norm/get-grtt-estimate session))
      ;; default
      nil
      )))

(defn record-bytes-sent
  "Record the number of bytes sent by this session"
  [session bytes-sent]
  (let [mbean (get @session-names session)]
    (jmx-write mbean :bytes-sent (+ (or (jmx/read mbean :bytes-sent) 0) bytes-sent))))

(defn record-bytes-received
  "Record the number of bytes received by this session"
  [session bytes-received]
  (let [mbean (get @session-names session)]
    (jmx-write mbean :bytes-received (+ (or (jmx/read mbean :bytes-received) 0) bytes-received))))

(defn record-number-of-sl-receivers
  [session sl-receivers]
  (let [mbean (get @session-names session)]
    (jmx-write mbean :sl-receiver-count sl-receivers)))

(defn mon-event-loop
  "Event handler for monitoring events. Subscribes to
  event-chan and uses all monitoring related events
  to update the session's JMX bean. This function uses
  a go task and returns immediately. The result is the
  event channel."
  [event-chan]
  (let [ec-tap (chan 5)]
    (log/trace "Enter mon event loop")
    (tap event-chan ec-tap)
    (go-loop [event (<! ec-tap)]
      (if event
        (do
          (update-mon-status event)
          (recur (<! ec-tap)))
        (do
          (untap event-chan ec-tap)
          (log/trace "Exit mon event loop"))))
    event-chan))

(defn register
  [session address port node-id]
  (let [mbean (jmx/create-bean (atom {}))
        s-name (str "com.senacor.msm:type=Session,"
                    "name=" address "/" port "/" node-id)]
    (swap! session-names assoc session s-name)
    (jmx/register-mbean mbean s-name)
    (log/tracef "JMX Session bean registered: %s" s-name)
    mbean))

(defn unregister
  [session]
  (log/tracef "JMX session unregistered: %s" (get @session-names session))
  (jmx/unregister-mbean (get @session-names session))
  (swap! session-names dissoc session))