(ns com.senacor.msm.norm.control
  (:require [com.senacor.msm.norm.norm-api :as norm]
            [clojure.tools.logging :as log]
            [clojure.java.jmx :as jmx]
            [clojure.string :as str]))

;;
;; Implements the NORM event listener and dispatches
;; norm status updates to registered senders and receivers
;;

;; maps a sending session to a callback function that handles the
;; sender-related NORM events
(def senders (atom {}))

;; maps a receiving session to a callback function that handles
;; receiver-related NORM events.
(def receivers (atom {}))

;; Session statistics used by JMX.
(def session-infos (atom {}))

(defn register-sender
  [session handler]
  (swap! senders assoc session handler))

(defn unregister-sender
  [session]
  (swap! senders dissoc session))

(defn register-receiver
  [session handler]
  (swap! receivers assoc session handler))

(defn unregister-receiver
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

(defn- is-sender-event?
  [event-type]
  (contains? #{:tx-queue-empty :tx-queue-vacancy} event-type))

(defn- is-receiver-event?
  [event-type]
  (str/starts-with? (str event-type) ":rx"))

(defn- is-monitored-event?
  [event-type]
  (contains? #{:tx-rate-changed :cc-active :cc-inactive :grtt-updated} event-type))

(defn- update-mon-status
  [event]
  (let [session (:session event)
        mbean (get @session-infos session)]
    (case (:event-type event)
      :tx-rate-changed (swap! session-infos
                              update-in [session :tx-rate] (norm/get-tx-rate session ))
      :cc-active (swap! session-infos
                        update-in [session :cc-active] true)
      :cc-inactive (swap! session-infos
                          update [session :cc-active] false)
      :grtt-updated (swap! session-infos
                           update-in [session :grtt] (norm/get-grtt-estimate session))
    )))

(defn- event-loop
  [instance]
  (log/trace "Enter event loop")
  (loop [event (norm/next-event instance)]
    (log/trace "Event loop:" event)
    (cond
      (is-sender-event? (:event-type event))
        (invoke-sender-callback event)
      (is-receiver-event? (:event-type event))
        (invoke-receiver-callback event)
      (is-monitored-event? (:event-type event))
        (update-mon-status event)
      )
    (recur (norm/next-event instance))))

(defn init-norm
  "Initialize the NORM infrastructure. Must be called
  exactly onece before any subsequent interaction is possible."
  [ ]
  (norm/create-instance))

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
    (let [mbean (jmx/create-bean (atom {}))]
      (swap! session-infos assoc session mbean)
      (jmx/register-mbean mbean
                          (str "com.senacor.msm.norm.control:name=Session/"
                               address "/" port "/" node-id)))
    (.start (Thread. (partial event-loop instance) "NORM event loop"))
    session))
