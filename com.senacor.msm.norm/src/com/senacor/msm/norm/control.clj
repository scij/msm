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

(def instance (atom 0))

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
    (log/trace "Event loop:" event)
    (cond
      (is-sender-event (:event-type event))
      (invoke-sender-callback event)
      (is-receiver-event (:event-type event))
      (invoke-receiver-callback event))
    (recur (norm/next-event instance))))

(defn get-instance [ ]
  @instance)

(defn init-norm
  "Initialize the NORM infrastructure. Must be called
  exactly onece before any subsequent interaction is possible."
  [ ]
  (assert (nil? @instance) "NORM already initialized")
  (swap! instance (fn [_] (norm/create-instance))))

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
  [address port node-id
   & {:keys [if-name ttl tos loopback]}]
  (let [session (norm/create-session @instance address port node-id)]
    (when if-name
      (norm/set-multicast-interface session if-name))
    (when ttl
      (norm/set-ttl session ttl))
    (when tos
      (norm/set-tos session tos))
    (when loopback
      (norm/set-loopback session true))
    (.start (Thread. event-loop "NORM event loop"))
    session))
