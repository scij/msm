(ns com.senacor.msm.norm.control
  (:require [com.senacor.msm.norm.norm-api :as norm]
            [com.senacor.msm.norm.mon :as mon]
            [clojure.tools.logging :as log]
            [clojure.java.jmx :as jmx]
            [clojure.string :as str]
            [clojure.core.async :refer [chan tap untap go-loop >! <! >!! close!]]))

;;
;; Implements the NORM event listener and dispatches
;; norm status updates to registered senders and receivers
;;


(defn- event-loop
  "Main event loop processing all events of a given instance and
  forwards them to an async channel. The loop terminates when an
  event-invalid pseudo event is received (which is posted after
  the instance has been shut down. This function loops until the
  event-invalid message has been received.
  instance NORM instance handle
  event-chan async channel that receives all events"
  [instance event-chan]
  (log/trace "Enter control event loop")
  (loop [event (norm/next-event instance)]
    (log/trace "Event:" event)
    (>!! event-chan event)
    (when (not= :event-invalid (:event-type event))
      (log/trace "wait for next event")
      (recur (norm/next-event instance))))
  (close! event-chan)
  (log/trace "Exit control event loop")
  event-chan)

(defn init-norm
  "Initialize the NORM infrastructure. Must be called
  exactly onece before any subsequent interaction is possible."
  [event-chan]
  (let [instance (norm/create-instance)
        event-loop-f (future (event-loop instance event-chan))]
    (norm/set-debug-level instance 2)
    instance))

(defn finit-norm
  [instance]
  (let [secs 20]
    (log/tracef "shutdown NORM in %d seconds" secs)
    (Thread/sleep (* secs 1000))
    (log/trace "shutdown now")
    (norm/stop-instance instance)
  ))

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
    (mon/register session address port node-id)
    session))
