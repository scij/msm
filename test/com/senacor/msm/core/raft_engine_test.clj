(ns com.senacor.msm.core.raft-engine-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [>!! <!! close! chan poll! timeout]]
            [melee.consensus :as mcons]
            [com.senacor.msm.core.raft-engine :refer :all]
            [com.senacor.msm.core.command :as command]
            [clojure.tools.logging :as log]
            [com.senacor.msm.core.monitor :as monitor]))

(deftest test-election-timeout
  (is (>= 300 (election-timeout)))
  (is (<= 150 (election-timeout)))
  (is (>= 300 (election-timeout)))
  (is (<= 150 (election-timeout)))
  (is (>= 300 (election-timeout)))
  (is (<= 150 (election-timeout)))
  (is (>= 300 (election-timeout)))
  (is (<= 150 (election-timeout)))
  (is (>= 300 (election-timeout)))
  (is (<= 150 (election-timeout)))
  (is (>= 300 (election-timeout)))
  (is (<= 150 (election-timeout)))
  (is (>= 300 (election-timeout)))
  (is (<= 150 (election-timeout)))
  (is (>= 300 (election-timeout)))
  (is (<= 150 (election-timeout)))
  (is (>= 300 (election-timeout)))
  (is (<= 150 (election-timeout)))
  (is (>= 300 (election-timeout)))
  (is (<= 150 (election-timeout)))
  (is (>= 300 (election-timeout)))
  (is (<= 150 (election-timeout)))
  (is (>= 300 (election-timeout)))
  (is (<= 150 (election-timeout)))
  (is (>= 300 (election-timeout)))
  (is (<= 150 (election-timeout)))
  (is (>= 300 (election-timeout)))
  (is (<= 150 (election-timeout)))
  (is (>= 300 (election-timeout)))
  (is (<= 150 (election-timeout))))

(deftest test-become-candidate
  (let [candidate (become-candidate (mcons/state 99 :follower 0 nil [] 0 0))]
    (is (= 1 (:current-term candidate)))
    (is (= :candidate (:role candidate)))))

(deftest test-become-follower
  (testing "From leader"
    (let [follower (become-follower (mcons/state 99 :leader 1 nil [] 1 1)
                                    {:cmd command/CMD_APPEND_ENTRIES,
                                     :current-term 3})]
      (is (= 3 (:current-term follower)))
      (is (= :follower (:role follower)))))
  (testing "From candidate"
    (let [follower (become-follower (mcons/state 99 :candidate 1 nil [] 1 1)
                                    {:cmd command/CMD_APPEND_ENTRIES,
                                     :current-term 3})]
      (is (= 3 (:current-term follower)))
      (is (= :follower (:role follower)))))
  )

(deftest test-heartbeat
  (is (= (command/parse-command (heartbeat "abc" (mcons/state "99" :leader 3 nil [] 1 1)))
         {:cmd command/CMD_APPEND_ENTRIES,
          :subscription "abc",
          :current-term 3,
          :leader-id "99",
          :prev-log-index 0,
          :prev-log-term 0,
          :leader-commit 1})))

(deftest test-raft-state-machine
  (with-redefs-fn {#'monitor/record-sf-status (fn [_ _])}
    #(do
      (testing "Startup case - starting as follower"
        (log/trace "***" *testing-contexts*)
        (let [cmd-chan-in (chan 1)
              cmd-chan-out (chan 1)
              a-leader? (atom false)]
          (is (= "99" (raft-state-machine "abc" "99" a-leader? cmd-chan-in cmd-chan-out)))
          (is (= (command/parse-command (<!! cmd-chan-out))
                 {:cmd command/CMD_REQUEST_VOTE,
                  :subscription "abc",
                  :term 1,
                  :candidate-id "99",
                  :last-log-index 0,
                  :last-log-term 0}))
          (is (not @a-leader?))
          (>!! cmd-chan-in :exit)))
      (testing "Election timeout"
        (log/trace "***" *testing-contexts*)
        (let [cmd-chan-in (chan 1)
              cmd-chan-out (chan 1)
              a-leader? (atom false)]
          (is (= "99" (raft-state-machine "abc" "99" a-leader? cmd-chan-in cmd-chan-out)))
          (is (= (command/parse-command (<!! cmd-chan-out))
                 {:cmd command/CMD_REQUEST_VOTE,
                  :subscription "abc",
                  :term 1,
                  :candidate-id "99",
                  :last-log-index 0,
                  :last-log-term 0}))
          (is (= (command/parse-command (<!! cmd-chan-out))
                 {:cmd command/CMD_APPEND_ENTRIES,
                  :subscription "abc",
                  :current-term 1,
                  :leader-id "99",
                  :prev-log-index 0,
                  :prev-log-term 0,
                  :leader-commit 0}))
          ; Shall become a leader after the election times out with no other message.
          (is @a-leader?)
          (>!! cmd-chan-in :exit)))
      (testing "Election winner"
        (log/trace "***" *testing-contexts*)
        (let [cmd-chan-in (chan 1)
              cmd-chan-out (chan 1)
              a-leader? (atom false)]
          (is (= "99" (raft-state-machine "abc" "99" a-leader? cmd-chan-in cmd-chan-out)))
          (is (= (command/parse-command (<!! cmd-chan-out))
                 {:cmd command/CMD_REQUEST_VOTE,
                  :subscription "abc",
                  :term 1,
                  :candidate-id "99",
                  :last-log-index 0,
                  :last-log-term 0}))
          (>!! cmd-chan-in (command/raft-vote-reply "abc" 1 "99" true))
          (is (= (command/parse-command (<!! cmd-chan-out))
                 {:cmd command/CMD_APPEND_ENTRIES,
                  :subscription "abc",
                  :current-term 1,
                  :leader-id "99",
                  :prev-log-index 0,
                  :prev-log-term 0,
                  :leader-commit 0}))
          (is @a-leader?)
          (>!! cmd-chan-in :exit)
          ))
      (testing "Sending heartbeats"
        (log/trace "***" *testing-contexts*)
        (let [cmd-chan-in (chan 1)
              cmd-chan-out (chan 1)
              a-leader? (atom false)]
          (is (= "99" (raft-state-machine "abc" "99" a-leader? cmd-chan-in cmd-chan-out)))
          (is (= (command/parse-command (<!! cmd-chan-out))
                 {:cmd command/CMD_REQUEST_VOTE,
                  :subscription "abc",
                  :term 1,
                  :candidate-id "99",
                  :last-log-index 0,
                  :last-log-term 0}))
          (>!! cmd-chan-in (command/raft-vote-reply "abc" 1 "99" true))
          (is (= (command/parse-command (<!! cmd-chan-out))
                 {:cmd command/CMD_APPEND_ENTRIES,
                  :subscription "abc",
                  :current-term 1,
                  :leader-id "99",
                  :prev-log-index 0,
                  :prev-log-term 0,
                  :leader-commit 0}))
          (is @a-leader?)
          (is (= (command/parse-command (<!! cmd-chan-out))
                 {:cmd command/CMD_APPEND_ENTRIES,
                  :subscription "abc",
                  :current-term 1,
                  :leader-id "99",
                  :prev-log-index 0,
                  :prev-log-term 0,
                  :leader-commit 0}))
          (is (= (command/parse-command (<!! cmd-chan-out))
                 {:cmd command/CMD_APPEND_ENTRIES,
                  :subscription "abc",
                  :current-term 1,
                  :leader-id "99",
                  :prev-log-index 0,
                  :prev-log-term 0,
                  :leader-commit 0}))
          (>!! cmd-chan-in :exit)))
      (testing "Another leader"
        (log/trace "***" *testing-contexts*)
        (let [cmd-chan-in (chan 1)
              cmd-chan-out (chan 2)
              a-leader? (atom false)]
          (is (= "99" (raft-state-machine "abc" "99" a-leader? cmd-chan-in cmd-chan-out)))
          (is (= (command/parse-command (<!! cmd-chan-out))
                 {:cmd command/CMD_REQUEST_VOTE,
                  :subscription "abc",
                  :term 1,
                  :candidate-id "99",
                  :last-log-index 0,
                  :last-log-term 0}))
          (>!! cmd-chan-in (command/raft-request-vote "abc" 1 "100" 0 0))
          (>!! cmd-chan-in (command/raft-request-vote "abc" 1 "100" 0 0))
          (is (not @a-leader?))
          (is (= (command/parse-command (<!! cmd-chan-out))
                 {:cmd command/CMD_VOTE_REPLY,
                  :candidate-id "100",
                  :subscription "abc",
                  :term 1,
                  :vote-granted false}))
          (>!! cmd-chan-in :exit)))
      (testing "Loosing leadership"
        (log/trace "***" *testing-contexts*)
        (let [cmd-chan-in (chan 1)
              cmd-chan-out (chan 1)
              a-leader? (atom false)]
          (is (= "99" (raft-state-machine "abc" "99" a-leader? cmd-chan-in cmd-chan-out)))
          (is (= (command/parse-command (<!! cmd-chan-out))
                 {:cmd            command/CMD_REQUEST_VOTE,
                  :subscription   "abc",
                  :term           1,
                  :candidate-id   "99",
                  :last-log-index 0,
                  :last-log-term  0}))
          (>!! cmd-chan-in (command/raft-append-entries "abc" 2 "100" 0 0 [] 0))
          (is (nil? (poll! cmd-chan-out)))
          (>!! cmd-chan-in :exit)))
      (testing "Not loosing leadership"
        (log/trace "***" *testing-contexts*)
        (let [cmd-chan-in (chan 1)
              cmd-chan-out (chan 1)
              a-leader? (atom false)]
          (is (= "99" (raft-state-machine "abc" "99" a-leader? cmd-chan-in cmd-chan-out)))
          (is (= (command/parse-command (<!! cmd-chan-out))
                 {:cmd            command/CMD_REQUEST_VOTE,
                  :subscription   "abc",
                  :term           1,
                  :candidate-id   "99",
                  :last-log-index 0,
                  :last-log-term  0}))
          (>!! cmd-chan-in (command/raft-vote-reply "abc" 1 "99" true))
          (is (= (command/parse-command (<!! cmd-chan-out))
                 {:cmd command/CMD_APPEND_ENTRIES,
                  :subscription "abc",
                  :current-term 1,
                  :leader-id "99",
                  :prev-log-index 0,
                  :prev-log-term 0,
                  :leader-commit 0}))
          (is @a-leader?)
          (>!! cmd-chan-in (command/raft-append-entries "abc" 1 "100" 0 0 [] 0))
          (is (= (command/parse-command (<!! cmd-chan-out))
                 {:cmd command/CMD_APPEND_ENTRIES,
                  :subscription "abc",
                  :current-term 1,
                  :leader-id "99",
                  :prev-log-index 0,
                  :prev-log-term 0,
                  :leader-commit 0}))
          (is @a-leader?)
          (>!! cmd-chan-in :exit)))
      (testing "Another subscription"
        (log/trace "***" *testing-contexts*)
        (let [cmd-chan-in (chan 1)
              cmd-chan-out (chan 1)
              a-leader? (atom false)]
          (is (= "99" (raft-state-machine "abc" "99" a-leader? cmd-chan-in cmd-chan-out)))
          (is (= (command/parse-command (<!! cmd-chan-out))
                 {:cmd command/CMD_REQUEST_VOTE,
                  :subscription "abc",
                  :term 1,
                  :candidate-id "99",
                  :last-log-index 0,
                  :last-log-term 0}))
          (>!! cmd-chan-in (command/raft-vote-reply "def" 1 "99" true))
          (is (not @a-leader?))
          (is (= (command/parse-command (<!! cmd-chan-out))
                 {:cmd command/CMD_APPEND_ENTRIES,
                  :subscription "abc",
                  :current-term 1,
                  :leader-id "99",
                  :prev-log-index 0,
                  :prev-log-term 0,
                  :leader-commit 0}))
          (>!! cmd-chan-in :exit)
          (is (nil? (<!! cmd-chan-out)))))
  )))

(deftest test-start-election
  (with-redefs-fn
    {#'election-timeout (fn [] 109),
     #'current-time (fn [] 1000)}
    #(do
       (is (= (start-election 123 "abc" 700)
              {:timeout-duration 109,
               :timeout-expiry 1109,
               :votes [{:subscription "abc",
                        :candidate-id 123,
                        :term 700,
                        :vote-granted true}]})))))

(deftest test-election-won?
  (testing "lonely server"
    (let [fix {:timeout-duration 0,
               :timeout-expiry 1109,
               :votes [{:subscription "abc",
                        :candidate-id 123,
                        :term 700,
                        :vote-granted true}]}]
      (is (election-won? fix 123))))
  (testing "Absolute majority"
    (let [fix {:timeout-duration 0,
               :timeout-expiry 1109,
               :votes [{:subscription "abc",
                        :candidate-id 123,
                        :term 700,
                        :vote-granted true},
                       {:subscription "abc",
                        :candidate-id 123,
                        :term 700,
                        :vote-granted true},
                       {:subscription "abc",
                        :candidate-id 123,
                        :term 700,
                        :vote-granted true}]}]
      (is (election-won? fix 123))))
  (testing "One vote for another server"
    (let [fix {:timeout-duration 0,
               :timeout-expiry 1109,
               :votes [{:subscription "abc",
                        :candidate-id 123,
                        :term 700,
                        :vote-granted true},
                       {:subscription "abc",
                        :candidate-id 123,
                        :term 700,
                        :vote-granted true},
                       {:subscription "abc",
                        :candidate-id 100,
                        :term 700,
                        :vote-granted true}]}]
      (is (election-won? fix 123))))
  (testing "Majority for another server"
    (let [fix {:timeout-duration 0,
               :timeout-expiry 1109,
               :votes [{:subscription "abc",
                        :candidate-id 100,
                        :term 700,
                        :vote-granted true},
                       {:subscription "abc",
                        :candidate-id 100,
                        :term 700,
                        :vote-granted true},
                       {:subscription "abc",
                        :candidate-id 123,
                        :term 700,
                        :vote-granted true}]}]
      (is (not (election-won? fix 123)))))
  (testing "Mixed vote"
    (let [fix {:timeout-duration 0,
               :timeout-expiry 1109,
               :votes [{:subscription "abc",
                        :candidate-id 100,
                        :term 700,
                        :vote-granted true},
                       {:subscription "abc",
                        :candidate-id 123,
                        :term 700,
                        :vote-granted false},
                       {:subscription "abc",
                        :candidate-id 123,
                        :term 700,
                        :vote-granted true}]}]
      (is (not (election-won? fix 123)))))
  )

(deftest test-register-vote
  (with-redefs-fn
    {#'current-time (fn [] 1010)}
    #(do
        (is (= (register-vote {:timeout-expiry 1019,
                               :timeout-duration 19,
                               :votes [{:subscription "abc",
                                        :candidate-id 123,
                                        :term 700,
                                        :vote-granted true}]},
                              {:subscription "abc",
                               :candidate-id 123,
                               :term 700,
                               :vote-granted true})
               {:timeout-expiry 1019,
                :timeout-duration 9,
                :votes [{:subscription "abc",
                         :candidate-id 123,
                         :term 700,
                         :vote-granted true},
                        {:subscription "abc",
                         :candidate-id 123,
                         :term 700,
                         :vote-granted true}]})))))