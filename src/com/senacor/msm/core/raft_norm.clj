(ns com.senacor.msm.core.raft-norm
  (:require [clojure.core.async :refer [>!! alts! go-loop timeout]]
            [melee.consensus :as mcons]
            [melee.log :as mlog]
            [com.senacor.msm.core.command :as command]))
;
; Implement the raft related IPC on top of NORM.
;

(defn election-timeout
  "returns the time the instance waits for other leaders reporting in msecs.
  The result contains a random component to reduce the chance for undecided elections"
  []
  (+ 100 (rand-int 120)))

(defn election
  [subscription state cmd-chan-in cmd-chan-out]
  (go-loop [[res c] (alts! [cmd-chan-in (timeout (+ (election-timeout) (election-timeout)))])]
    (cond
      ;; timeout waiting vor reply -> start a new election
      (nil? res) (recur (alts! [cmd-chan-in (timeout (+ (election-timeout) (election-timeout)))]))
      ))
  ; nil huh?
  ; request-vote someone else is the leader
  ; request-vote-reply counting
  )

(defn become-candidate
  [state]
  (mcons/state (:id state) :candidate (inc (:current-term state))
               (:id state) (:log state) (:commit-index state) (:last-applied state)))

(defn follow-the-leader
  "Listens for heartbeats and log updates. Starts a new election when nothing is received."
  [subscription state cmd-chan-in cmd-chan-out]
  (go-loop [[res c] (alts! [cmd-chan-in (timeout (election-timeout))])]
    (cond

      (nil? res) (election subscription (become-candidate state) cmd-chan-in cmd-chan-out)
      (empty? (:entries res)) (recur (alts! (cmd-chan-in (timeout (election-timeout)))))
      :else (recur (alts! (cmd-chan-in (timeout (election-timeout))))))
    ))

(defn request-vote
  [subscription state cmd-chan-out]
  (>!! cmd-chan-out (command/raft-request-vote subscription
                                               (:current-term state)
                                               (:id state)
                                               (count (:log state))
                                               (mlog/last-term (:log state)))))

(defn heartbeat
  [subscription state cmd-chan-out]
  (>!! cmd-chan-out (command/raft-append-entries subscription
                                                 (:current-term state)
                                                 (:id state)
                                                 (count (:log state))
                                                 (mlog/last-term (:log state))
                                                 []
                                                 (:commit-index state))))

(defn raft-lifecycle
  [subscription session-id a-leader? cmd-chan-in cmd-chan-out]
  (let [my-state (mcons/state session-id :follower 0 nil [] 0 0)]
    (follow-the-leader subscription my-state cmd-chan-in cmd-chan-out)
    ))