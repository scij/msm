(ns com.senacor.msm.core.control
  (:require [com.senacor.msm.core.norm-api :as norm]
            [com.senacor.msm.core.monitor :as mon]
            [clojure.tools.logging :as log]
            [clojure.java.jmx :as jmx]
            [clojure.string :as str]
            [clojure.core.async :refer [chan tap untap go-loop thread >! <! >!! close!]]))


;;
;; Implements the NORM event listener and dispatches
;; norm status updates to registered senders and receivers
;;


(defn event-loop
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
    (log/trace "Event received:" (norm/event->str event))
    (>!! event-chan event)
    (when (not= :event-invalid (:event-type event))
      (recur (norm/next-event instance))))
  (close! event-chan)
  (log/trace "Exit control event loop")
  event-chan)

(defn init-norm
  "Initialize the NORM infrastructure. Must be called
  exactly onece before any subsequent interaction is possible."
  [event-chan]
  (let [instance (norm/create-instance)
        event-loop-result-chan (thread (event-loop instance event-chan))]
    (norm/set-debug-level instance 2)
    instance))

(defn finit-norm
  [instance]
  (norm/stop-instance instance))


(defn start-session
  "Starts a NORM session for sending and/or receiving data.
  Returns the session handle.
  address is the multicast address of the service
  port is the UDP network port to use
  options is a map of additional network options as described below:
  :if-name <str> name of the network interface to bind to.
  :ttl <byte> number of hops a packet will survive.
  :tos <byte> the type of service value for all packets
  :loopback if set will enable loopback i.e. local communication on this host
  :node-id <int> is a numeric argument to distinguish this service. If
  omitted the process id will be used. Override it if you have more than
  one session per process."
  [instance if-name address port options]
  (let [session (norm/create-session instance address port (:node-id options))]
    (log/infof "session created %s %d %d" address port (:node-id options))
    (when-not (str/blank? if-name)
      (norm/set-multicast-interface session if-name))
    (when (:ttl options)
      (norm/set-ttl session (:ttl options)))
    (when (:tos options)
      (norm/set-tos session (:tos options)))
    (when (:loopback options)
      (log/trace "loopback set")
      (norm/set-loopback session true)
      (norm/set-rx-port-reuse session true))
    (mon/register session address port (:node-id options))
    session))

(defn stop-session
  "Stops and unregisters the session."
  [session]
  (norm/destroy-session session)
  (mon/unregister session)
  (log/info "session closed" session))
