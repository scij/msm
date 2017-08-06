(ns com.senacor.msm.norm.core.monitor
  (:require [clojure.java.jmx :as jmx]
            [com.senacor.msm.norm.core.norm-api :as norm]
            [clojure.core.async :refer [chan go-loop tap <! untap]]
            [clojure.tools.logging :as log]))

;; Maps sessions to JMX bean names.
(def session-names (atom {}))

(defn- jmx-write
  [mbean prop value]
  (log/tracef "jmx write %s" (str prop))
  (jmx/write! mbean prop value))

(defn- update-mon-status
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
      (log/trace "Event:" event)
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
        s-name (str "com.senacor.msm.norm.control:name=Session/"
                    address "/" port "/" node-id)]
    (swap! session-names assoc session s-name)
    (jmx/register-mbean mbean s-name)
    (log/tracef "JMX Session bean registered: %s" s-name)))

(defn unregister
  [session]
  (log/tracef "JMX session unregistered: %s" (get @session-names session))
  (jmx/unregister-mbean (get @session-names session)))