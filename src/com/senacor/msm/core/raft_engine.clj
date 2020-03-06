(ns com.senacor.msm.core.raft-engine
  (:require [clojure.core.async :refer [>!! >! alts! chan close! go-loop pipeline timeout]]
            [melee.consensus :as mcons]
            [melee.log :as mlog]
            [com.senacor.msm.core.command :as command]
            [clojure.tools.logging :as log]
            [com.senacor.msm.core.monitor :as mon]))
;
; Implement a raft engine on top of the melee implementation.
; This implementation uses NORM commands to exchange state between nodes.
;

(def ^:const heartbeat-interval
  "Time in ms between heartbeats sent out by the leader"
  100)

(defn election-timeout
  "returns the time the instance waits for other leaders reporting in msecs.
  The result contains a random component to reduce the chance for undecided elections"
  []
  (+ 150 (rand-int 150)))

(defn current-time
  "Returns current time in milliseconds. This function actually exists to
  mock currentTimeMillis and make testing easier"
  []
  (System/currentTimeMillis))

(defn command-append-entries?
  "Returns true if the response contains the append-entries command"
  [res]
  (= command/CMD_APPEND_ENTRIES (:cmd res)))

(defn command-request-vote?
  "Returns true if the response contains a request vote command"
  [res]
  (= command/CMD_REQUEST_VOTE (:cmd res)))

(defn command-vote-reply?
  "Returns true if the response contains a vote reply command"
  [res]
  (= command/CMD_VOTE_REPLY (:cmd res)))

(defn become-candidate
  [state]
  (mcons/state (:id state) :candidate (inc (:current-term state))
               (:id state) (:log state) (:commit-index state) (:last-applied state)))

(defn become-follower
  [state append-request]
  (assert (command-append-entries? append-request))
  (mcons/state (:id state) :follower (:current-term append-request) (:voted-for nil) (:log state)
               (:leader-commit append-request) (:leader-commit append-request)))

(defn follower?
  "Returns true if the state is follower"
  [state]
  (= :follower (:role state)))

(defn leader?
  "Returns true if the state is leader"
  [state]
  (= :leader (:role state)))

(defn candidate?
  "Returns true if the state is candidate"
  [state]
  (= :candidate (:role state)))

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

(defn start-election
  "Creates a new election state map with timeout initialized and
  an initial vote for the initiating session."
  [session subscription term]
  (let [e-timeout (election-timeout)]
    {:timeout-duration e-timeout,
     :timeout-expiry (+ (current-time) e-timeout),
     :votes [{:subscription subscription,
              :term term,
              :candidate-id session,
              :vote-granted true}]}))

(defn register-vote
  "Returns a new election state representing remaining election
  time and the votes collected so far
  state is the election status. May be nil if the election has just been started
  vote is a vote-reply just received. May be nil if the election timed out.
  Returns an updated election state"
  [state vote-reply]
  {:timeout-duration (- (:timeout-expiry state) (current-time)),
   :timeout-expiry (:timeout-expiry state),
   :votes (conj (:votes state) vote-reply)})

(defn election-won?
  "Checks if session has won the majority of votes in the election
   state is the election state.
   session is the identifier of the current session"
  [state session]
  (> (count (filter #(and (= session (:candidate-id %))
                          (:vote-granted %))
                    (:votes state)))
     (/ (count (:votes state)) 2)))

(defn ballot
  "Create a ballot from the vote-request result"
  [vote-reply]
  (assert (command-request-vote? vote-reply))
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
              wait-time (election-timeout)
              election-state nil]
      (let [[cmd chan] (alts! [parsed-cmd-chan (timeout wait-time)])]
        (log/trace "State machine loop" my-state cmd)
        (mon/record-sf-status session-id (str my-state))
        (cond
          ; Any state
          ; Kill switch
          (and (= chan parsed-cmd-chan) (= cmd :exit))
          (do
            (log/trace "Exit command received. Raft state machine exiting")
            (close! cmd-chan-out))
          ; Different subscription
          (and (some? cmd) (not= subscription (:subscription cmd)))
          (recur my-state wait-time election-state)
          ; Received vote request
          (and (command-request-vote? cmd) (= subscription (:subscription cmd)))
          (let [vote-result (mcons/vote my-state (ballot cmd))]
            (log/trace "Voting" vote-result)
            (>! cmd-chan-out (command/raft-vote-reply subscription (:term vote-result) (:candidate-id cmd)
                                                      (:vote-granted vote-result)))
            (recur (:state vote-result) wait-time election-state))

          ; State = Follower
          ; Timed out waiting for heartbeat -> start new election
          (and (follower? my-state) (nil? cmd))
          (let [candidate-state (become-candidate my-state)]
            (log/trace "Starting election")
            (>! cmd-chan-out (command/raft-request-vote subscription
                                                        (:current-term candidate-state)
                                                        (:id candidate-state)
                                                        (count (:log candidate-state))
                                                        (mlog/last-term (:log candidate-state))))
            (recur candidate-state
                   (election-timeout)
                   (start-election (:id candidate-state) subscription (:current-term candidate-state))))
          ; Received heartbeat.
          (and (follower? my-state) (command-append-entries? cmd)
               (empty? (:entries cmd)) (= subscription (:subscription cmd)))
          (recur (mcons/state (:id my-state) :follower (max (:current-term cmd) (:current-term my-state))
                              (:voted-for my-state) (:log my-state) (:commit-index my-state) (:last-applied my-state))
                 heartbeat-interval
                 election-state)

          ; State = Candidate
          ; Timeout in election, restart election
          (and (candidate? my-state) (nil? cmd))
          (if (election-won? election-state (:id my-state))
            (let [leader (mcons/state (:id my-state)
                                      :leader
                                      (:current-term my-state)
                                      nil ;voted for
                                      (:log my-state)
                                      (:commit-index my-state)
                                      (:last-applied my-state))]
              (log/info "Election won" leader)
              (reset! a-leader? true)
              (>! cmd-chan-out (heartbeat subscription leader))
              (recur leader heartbeat-interval nil)))
          ; Vote reply received
          ; todo count votes and check majority
          (and (candidate? my-state) (command-vote-reply? cmd) (= subscription (:subscription cmd)))
          (let [upd-election-state (register-vote election-state cmd)]
            (log/trace "Vote received" election-state cmd)
            (recur my-state (:timeout-duration election-state) upd-election-state))
          ; someone else won the election
          (and (candidate? my-state) (command-append-entries? cmd) (= subscription (:subscription cmd)))
          (if (>= (:current-term cmd) (:current-term my-state))
            (recur (become-follower my-state cmd) heartbeat-interval nil)
            (recur my-state (:timeout-duration election-state) election-state))

          ; State = Leader
          ; Leader timeout - send new heartbeat.
          (and (leader? my-state) (nil? cmd))
          (do
            (>! cmd-chan-out (heartbeat subscription my-state))
            (recur my-state heartbeat-interval election-state))
          ; There is another leader
          (and (leader? my-state) (command-append-entries? cmd) (= subscription (:subscription cmd)))
          (if (> (:current-term cmd) (:current-term my-state))
            (do
              (log/info "Fallback to follower. New leader is " (:leader-id cmd))
              (reset! a-leader? false)
              (recur (become-follower my-state cmd) (election-timeout) election-state))
            (recur my-state heartbeat-interval election-state))

          :else
          (do
            (log/error "Unknown state transition: " my-state cmd)
            (close! cmd-chan-out))
          ))))
  session-id)