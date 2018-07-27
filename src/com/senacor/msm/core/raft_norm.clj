(ns com.senacor.msm.core.raft-norm
  (:require [clojure.core.async :refer [>!! >! alts! chan close! go-loop pipeline timeout]]
            [melee.consensus :as mcons]
            [melee.log :as mlog]
            [com.senacor.msm.core.command :as command]
            [clojure.tools.logging :as log]))
;
; Implement the raft related IPC on top of NORM.
;

(def ^:const heartbeat-interval
  "Time in ms between heartbeats sent out by the leader"
  100)

(defn election-timeout
  "returns the time the instance waits for other leaders reporting in msecs.
  The result contains a random component to reduce the chance for undecided elections"
  []
  (+ 150 (rand-int 150)))

(defn become-candidate
  [state]
  (mcons/state (:id state) :candidate (inc (:current-term state))
               (:id state) (:log state) (:commit-index state) (:last-applied state)))

(defn become-follower
  [state append-request]
  (assert (= command/CMD_APPEND_ENTRIES (:cmd append-request)))
  (mcons/state (:id state) :follower (:current-term append-request) (:voted-for nil) (:log state)
               (:leader-commit append-request) (:leader-commit append-request)))

(defn heartbeat
  "Returns the heartbeat message for the current state."
  [subscription state]
  (assert (= :leader (:role state)))
  (log/trace "Sending heartbeat" state)
  (command/raft-append-entries subscription
                               (:current-term state)
                               (:id state)
                               (count (:log state))
                               (mlog/last-term (:log state))
                               []
                               (:commit-index state)))

(defn ballot
  "Create a ballot from the vote-request result"
  [vote-reply]
  (assert (= command/CMD_REQUEST_VOTE (:cmd vote-reply)))
  (mcons/ballot (:term vote-reply) (:candidate-id vote-reply) (:last-log-index vote-reply) (:last-log-term vote-reply)))

(defn raft-state-machine
  "Communicate with other services on the same subscription and determine leaders and followers.
  Implements the Raft protocol.
  subscription Only considers processes on the same subscription.
  session-id Identity of the current process. Returns the session-id.
  a-leader? a boolean encapsulated in an atom to indicate whether this process is the current leader.
  cmd-chan-in NORM inbound command channel where Raft messages from other processes are received.
  cmd-chan-out NORM output command channel where the process sends Raft messages to other processes."
  [subscription session-id a-leader? cmd-chan-in cmd-chan-out]
  (reset! a-leader? false)
  (let [parsed-cmd-chan (chan 1)]
    (pipeline 1 parsed-cmd-chan (map command/parse-command) cmd-chan-in)
    (go-loop [my-state (mcons/state session-id :follower 0 nil [] 0 0)
              wait-time (election-timeout)]
      (let [[res chan] (alts! [parsed-cmd-chan (timeout wait-time)])]
        (log/trace "State machine loop" my-state res)
        (cond
          ; Any state
          ; Kill switch
          (and (= chan parsed-cmd-chan) (= res :exit))
          (do
            (log/trace "Exit command received. Raft state machine exiting")
            (close! cmd-chan-out))
          ; Different subscription
          (and (some? res) (not= subscription (:subscription res)))
          (recur my-state wait-time)
          ; Received vote request
          (and (= command/CMD_REQUEST_VOTE (:cmd res)) (= subscription (:subscription res)))
          (let [vote-result (mcons/vote my-state (ballot res))]
            (log/trace "Voting" vote-result)
            (>! cmd-chan-out (command/raft-vote-reply subscription (:term vote-result) (:candidate-id res)
                                                      (:vote-granted vote-result)))
            (recur (:state vote-result) wait-time))

          ; State = Follower
          ; Timed out waiting for heartbeat -> start new election
          (and (= :follower (:role my-state)) (nil? res))
          (let [candidate-state (become-candidate my-state)]
            (log/trace "Starting election")
            (>! cmd-chan-out (command/raft-request-vote subscription
                                                        (:current-term candidate-state)
                                                        (:id candidate-state)
                                                        (count (:log candidate-state))
                                                        (mlog/last-term (:log candidate-state))))
            (recur candidate-state (election-timeout)))
          ; Received heartbeat.
          (and (= :follower (:role my-state)) (= command/CMD_APPEND_ENTRIES (:cmd res))
               (empty? (:entries res)) (= subscription (:subscription res)))
          (recur (mcons/state (:id my-state) :follower (max (:current-term res) (:current-term my-state))
                              (:voted-for my-state) (:log my-state) (:commit-index my-state) (:last-applied my-state))
                 (election-timeout))

          ; State = Candidate
          ; Timeout in election, restart election
          (and (= :candidate (:role my-state)) (nil? res))
          (let [candidate-state (become-candidate my-state)]
            (log/trace "Restarting election after timeout")
            (>! cmd-chan-out (command/raft-request-vote subscription
                                                        (:current-term candidate-state)
                                                        (:id candidate-state)
                                                        (count (:log candidate-state))
                                                        (mlog/last-term (:log candidate-state))))
            (recur candidate-state (election-timeout)))
          ; Vote reply received
          (and (= :candidate (:role my-state)) (= command/CMD_VOTE_REPLY (:cmd res)) (= subscription (:subscription res)))
          ; todo count votes and check majority
          (if (= (:id my-state) (:candidate-id res))
            (let [leader (mcons/state (:id my-state) :leader (max (:current-term my-state) (:term res))
                                      (:voted-for nil) (:log my-state) (:commit-index my-state) (:last-applied my-state))]
              (log/info "Becoming leader" leader)
              (reset! a-leader? true)
              (>! cmd-chan-out (heartbeat subscription leader))
              (recur leader heartbeat-interval))
            (recur my-state (election-timeout)))
          ; someone else won the election
          (and (= :candidate (:role my-state)) (= command/CMD_APPEND_ENTRIES (:cmd res)) (= subscription (:subscription res)))
          (if (>= (:current-term res) (:current-term my-state))
            (recur (become-follower my-state res)
                   (election-timeout))
            (recur my-state (election-timeout)))

          ; State = Leader
          ; Leader timeout - send new heartbeat.
          (and (= :leader (:role my-state)) (nil? res))
          (do
            (>! cmd-chan-out (heartbeat subscription my-state))
            (recur my-state heartbeat-interval))
          ; There is another leader
          (and (= :leader (:role my-state)) (= command/CMD_APPEND_ENTRIES (:cmd res))
               (= subscription (:subscription res)))
          (if (> (:current-term res) (:current-term my-state))
            (do
              (log/info "Fallback to follower. New leader is " (:leader-id res))
              (reset! a-leader? false)
              (recur (become-follower my-state res) (election-timeout)))
            (recur my-state heartbeat-interval))

          :else
          (do
            (log/error "Unknown state transition: " my-state res)
            (close! cmd-chan-out))
          ))))
  session-id)