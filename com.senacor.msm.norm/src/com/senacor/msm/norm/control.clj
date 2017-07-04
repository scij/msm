(ns com.senacor.msm.norm.control
  (:require [com.senacor.msm.norm.norm-api :as norm]
            [clojure.tools.logging :as log]
            [clojure.java.jmx :as jmx]
            [clojure.string :as str]
            [clojure.core.async :refer [chan go-loop >! <! close!]]))

;;
;; Implements the NORM event listener and dispatches
;; norm status updates to registered senders and receivers
;;

;; Maps sessions to JMX bean names.
(def session-names (atom {}))

(defn- update-mon-status
  [event]
  (let [session (:session event)
        mbean (get @session-names session)]
    (case (:event-type event)
      :tx-rate-changed (jmx/write! mbean :tx-rate (norm/get-tx-rate session ))
      :cc-active (jmx/write! mbean :cc-active true)
      :cc-inactive (jmx/write! mbean :cc-active false)
      :grtt-updated (jmx/write! mbean :grtt (norm/get-grtt-estimate session))
      ;; default
      nil
    )))

(defn- event-loop
  [instance event-chan]
  (log/trace "Enter event loop")
  (go-loop [event (norm/next-event instance)]
    (log/trace "Event:" event)
    (when (not= :event-invalid (:event-type event))
      (>! event-chan event)
      (recur (norm/next-event instance)))))

(defn mon-event-loop
  [event-chan]
  (go-loop [event (<! event-chan)]
    (update-mon-status event)
    (recur (<! event-chan))))

(defn init-norm
  "Initialize the NORM infrastructure. Must be called
  exactly onece before any subsequent interaction is possible."
  [event-chan]
  (let [instance (norm/create-instance)]
    (event-loop instance event-chan)
    instance))

(defn finit-norm
  [instance]
  (log/trace "shutdown NORM")
  (Thread/sleep 5000)
  (norm/stop-instance instance))

(defn start-norm-session
  "Starts a NORM session for sending and/or receiving data.
  Returns the session handle.
  address is the multicast address of the service
  port is the UDP network port to use
  node-id is a numeric argument to distinguish this service
  The following keyword args are supported
  :if-name <str> name of the network interface to bind to.
  :ttl <byte> number of hops a packet will survive.
  :tos <byte> the type of service value for all packets
  :loopback if set will enable loopback i.e. local communication
  on this host"
  [instance address port node-id
   & {:keys [if-name ttl tos loopback]}]
  (let [session (norm/create-session instance address port node-id)]
    (when if-name
      (norm/set-multicast-interface session if-name))
    (when ttl
      (norm/set-ttl session ttl))
    (when tos
      (norm/set-tos session tos))
    (when loopback
      (log/trace "loopback set")
      (norm/set-loopback session true)
      (norm/set-rx-port-reuse session true))
    (let [mbean (jmx/create-bean (atom {}))
          s-name (str "com.senacor.msm.norm.control:name=Session/"
                      address "/" port "/" node-id)]
      (swap! session-names assoc session s-name)
      (jmx/register-mbean mbean s-name))
    session))
