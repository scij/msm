(ns com.senacor.msm.core.raft-norm-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [>!! <!! close! chan timeout]]
            [melee.consensus :as mcons]
            [com.senacor.msm.core.raft-norm :refer :all]
            [com.senacor.msm.core.command :as command]))

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
  (testing "Startup case - starting as follower"
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
             {:cmd command/CMD_REQUEST_VOTE,
              :subscription "abc",
              :term 2,
              :candidate-id "99",
              :last-log-index 0,
              :last-log-term 0}))
      (is (not @a-leader?))
      (>!! cmd-chan-in :exit)))
  (testing "Election winner"
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
      (is (not @a-leader?))
      (is (= (command/parse-command (<!! cmd-chan-out))
             {:cmd command/CMD_VOTE_REPLY,
              :candidate-id "100",
              :subscription "abc",
              :term 1,
              :vote-granted false}))
      (is (= (command/parse-command (<!! cmd-chan-out))
             {:cmd command/CMD_REQUEST_VOTE,
              :subscription "abc",
              :term 2,
              :candidate-id "99",
              :last-log-index 0,
              :last-log-term 0}))
      (>!! cmd-chan-in :exit)))
  (testing "Another subscription"
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
             {:cmd command/CMD_REQUEST_VOTE,
              :subscription "abc",
              :term 2,
              :candidate-id "99",
              :last-log-index 0,
              :last-log-term 0}))
      (>!! cmd-chan-in :exit)))
  )