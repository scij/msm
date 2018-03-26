(ns com.senacor.msm.core.stateless-test
  (:require [clojure.test :refer :all]
            [com.senacor.msm.core.stateless :refer :all]
            [clojure.core.async :refer [go-loop onto-chan close! chan onto-chan timeout pipeline poll! <! >! <!! >!!]]
            [com.senacor.msm.core.command :as command]
            [com.senacor.msm.core.norm-api :as norm]
            [com.senacor.msm.core.message :as message]
            [com.senacor.msm.core.monitor :as monitor]
            [com.senacor.msm.core.receiver :as receiver]
            [clojure.tools.logging :as log]
            [me.raynes.moments :as moments]))

(deftest test-alive-sessions
  (let [fix {1 {:expires 100, :subscription "100"},
             2 {:expires 200, :subscription "200"},
             3 {:expires 300, :subscription "300"},
             4 {:expires 400, :subscription "400"}}]
    (is (= {3 {:expires 300, :subscription "300"},
            4 {:expires 400, :subscription "400"}}
           (alive-sessions fix 200)))))

(deftest test-session-is-alive
  (testing "update existing session"
    (let [fix {1 {:expires 100, :subscription "s100"},
               2 {:expires 200, :subscription "s200"}}]
      (is (= {1 {:expires 100, :subscription "s100"},
              2 {:expires 500, :subscription "s200"}}
             (session-is-alive fix 2 "s200" 300)))))
  (testing "add new session"
    (let [fix {1 {:expires 100, :subscription "s100"},
               2 {:expires 200, :subscription "s200"}}]
      (is (= {1 {:expires 100, :subscription "s100"},
              2 {:expires 200, :subscription "s200"}
              3 {:expires 500, :subscription "s300"}}
             (session-is-alive fix 3 "s300" 300)))))
  )

(deftest test-find-my-index
  (testing "one entry"
    (let [fix (sorted-map my-session {:expires 500, :subscription "smy"})]
      (is (= 0 (find-my-index 0 fix)))))
  (testing "two entries, my session 2nd"
    (let [fix (sorted-map my-session {:expires 500, :subscription "smy"}
                          "a-session" {:expires 600, :subscription "s600"})]
      (is (= 1 (find-my-index 0 fix)))))
  (testing "two entries, my session 1st"
    (let [fix (sorted-map my-session {:expires 500, :subscription "smy"}
                          "new-session" {:expires 600, :subscription "s600"})]
      (is (= 0 (find-my-index 0 fix)))))
  (testing "three entries, my session in the middle"
    (let [fix (sorted-map my-session {:expires 500, :subscription "smy"}
                          "new-session" {:expires 600, :subscription "s600"}
                          "a-session" {:expires 300, :subscription "s300"})]
      (is (= 1 (find-my-index 0 fix))))))

(deftest test-handle-receiver-status
  (with-redefs-fn {#'norm/get-local-node-id (fn [sess] sess),
                   #'norm/get-node-name (fn [node] (str node)),
                   #'monitor/record-number-of-sl-receivers (fn [_ _])}
    #(do
       (testing "add another receiver"
         (let [my-session-index (atom 0)
               receiver-count (atom 1)
               session-receivers (atom {my-session {:expires Long/MAX_VALUE}})
               cmd-chan-in (chan 1)
               task (handle-receiver-status 1 "label" cmd-chan-in session-receivers my-session-index receiver-count)]
           (Thread/sleep 10)
           (is (= 1 @receiver-count))
           (>!! cmd-chan-in {:cmd command/CMD_ALIVE,
                             :active true,
                             :subscription "label"
                             :session-index 1,
                             :msg-seq-nbr 1000,
                             :node-id "remote:3456"})
           (Thread/sleep 100) ; todo add a better way to synchronize
           (receiver-status-housekeeping 1 session-receivers receiver-count my-session-index)
           (is (= 0 @my-session-index))
           (is (= 2 @receiver-count))
           (close! cmd-chan-in)
           ))
       (testing "add another receiver with a lower session id"
         (let [my-session-index (atom 0)
               receiver-count (atom 1)
               session-receivers (atom {my-session {:expires Long/MAX_VALUE}})
               cmd-chan-in (chan 1)
               task (handle-receiver-status 2 "label" cmd-chan-in session-receivers my-session-index receiver-count)]
           (Thread/sleep 10)
           (is (= 1 @receiver-count))
           (>!! cmd-chan-in {:cmd command/CMD_ALIVE,
                             :active true,
                             :subscription "label",
                             :session-index @my-session-index,
                             :msg-seq-nbr 1000,
                             :node-id "aaa:3456"})
           (Thread/sleep 100) ; todo add a better way to synchronize
           (receiver-status-housekeeping 1 session-receivers receiver-count my-session-index)
           (is (= 1 @my-session-index))
           (is (= 2 @receiver-count))
           (close! cmd-chan-in)
           ))
       (testing "expire another receiver"
         (let [my-session-index (atom 0)
               receiver-count (atom 1)
               session-receivers (atom {my-session {:expires Long/MAX_VALUE}})
               cmd-chan-in (chan 1)
               task (handle-receiver-status 2  "label" cmd-chan-in session-receivers my-session-index receiver-count)]
           (>!! cmd-chan-in {:cmd command/CMD_ALIVE,
                             :subscription "label",
                             :active true,
                             :session-id 0,
                             :msg-seq-nbr 1000,
                             :node-id "remote:3456"})
           (Thread/sleep 100) ; todo add a better way to synchronize
           (receiver-status-housekeeping 1 session-receivers receiver-count my-session-index)
           (is (= 2 @receiver-count))
           (is (= 0 @my-session-index))
           (Thread/sleep (+ expiry-threshold 10))
           (receiver-status-housekeeping 1 session-receivers receiver-count my-session-index)
           (is (= 1 @receiver-count))
           (is (= 0 @my-session-index))
           ))
       )))

(deftest test-filter-my-messages
  (let [fix (message/create-message "abc" "def" 4 "payload")
        zero (atom 0)
        two (atom 2)
        four (atom 4)
        seq-no-chan (chan 1)
        one (atom 1)]
    (testing "match"
      (is (is-my-message seq-no-chan zero four fix)))
    (testing "wrong index"
      (is (not (is-my-message seq-no-chan two four fix))))
    (testing "only one receiver"
      (is (is-my-message seq-no-chan zero one fix)))
    ))

(deftest test-prepare-to-join
  (testing "simple case without timing"
    (let [c (chan 20)
          a-join-seq-no (atom 10000)]
      (onto-chan c (range 100 121))
      (is (= a-join-seq-no(prepare-to-join c a-join-seq-no)))
      (Thread/sleep 10)
      (is (= 140 @a-join-seq-no))))
  (testing "simple case with timeout"
    (let [c (timeout 50)
          a-join-seq-no (atom 10000)]
      (onto-chan c (range 100 121))
      (is (= a-join-seq-no (prepare-to-join c a-join-seq-no)))
      (Thread/sleep 10)
      (is (= 140 @a-join-seq-no))))
  (testing "true timeout"
    (let [c (timeout 250)
          a-join-seq-no (atom 10000)]
      (go-loop [s 100]
        (<! (timeout 25))
        ;(Thread/sleep 50)
        (when (>! c s)
          (recur (inc s))))
      (is (= a-join-seq-no (prepare-to-join c a-join-seq-no)))
      (Thread/sleep 300)
      (is (not= 10000 @a-join-seq-no))))
  )

(deftest test-stateless-session-handler
  (testing "normal session startup and one message"
    (let [fix-msgs1 (map #(message/create-message "s1" (str "co" %) (+ 1000 %) (str "Payload " %))
                         (range 1 21))
          fix-msgs2 (map #(message/create-message "s1" (str "co" %) (+ 1000 %) (str "Payload " %))
                         (range 40 60))
          fix-cmd (map #(hash-map :cmd command/CMD_ALIVE,
                                  :subscription "s1",
                                  :active true,
                                  :node-id (str (+ 100 %)),
                                  :msg-seq-nbr (+ 1000 %))
                       (range 1 6))]
      (with-redefs-fn {#'command/command-receiver (fn [session event-chan cmd-chan]),
                       #'command/command-sender   (fn [session event-chan cmd-chan]),
                       #'norm/get-node-name       (fn [node-id]
                                                    node-id),
                       #'norm/get-local-node-id   (fn [session] 1),
                       #'norm/start-sender        (fn [_ _ _ _ _ _]),
                       #'moments/schedule-every   (fn [_ _ _]),
                       #'prepare-to-join          (fn [seq-no-chan a-join-seq-no]
                                                    (go-loop [s (<! seq-no-chan)]
                                                      (when s
                                                        (recur (<! seq-no-chan))))
                                                    (log/trace "join with 1020")
                                                    (reset! a-join-seq-no 1020)),
                       #'handle-receiver-status   (fn [session label cmd-chan-in a-session-receivers a-my-session-index a-receiver-count]
                                                     (reset! a-my-session-index 0)),
                       #'monitor/record-number-of-sl-receivers
                                                  (fn [_ rec-count]
                                                    (log/tracef "record number of receivers %d"
                                                                rec-count)),
                       #'receiver/create-receiver (fn [session event-chan msg-chan msg-filter]
                                                    (log/trace "creating fake receiver")
                                                    (let [m-chan (chan 20)]
                                                      (pipeline 1 msg-chan msg-filter m-chan)
                                                      (log/trace "pushing init messages")
                                                      (onto-chan m-chan (map message/Message->bytes fix-msgs1) false)
                                                      ;(log/trace "wait to join")
                                                      ;(<!! (timeout (* 3 alive-interval)))
                                                      ;(Thread/sleep (* 3 alive-interval))
                                                      (log/trace "pushing data messages")
                                                      (onto-chan m-chan (map message/Message->bytes fix-msgs2))
                                                      (log/trace "push complete")))}
      #(let [msg-chan (timeout 1000)]
         (stateless-session-handler 1 "s1" nil msg-chan)
         (is (= "Payload 40" (:payload (<!! msg-chan))))
        )))))

(deftest test-is-my-message
  (let [fix (message/create-message "abc" "def" 1005 "payload")
        a-my-index1 (atom 1)
        a-my-index2 (atom 0)
        a-receiver-count (atom 4)]
    (testing "is my message"
      (is (is-my-message nil a-my-index1 a-receiver-count fix)))
    (testing "is not my message"
      (is (not (is-my-message nil a-my-index2 a-receiver-count fix))))
    (testing "not initialized"
      (let [c (chan 1)]
        (is (not (is-my-message c (atom nil) a-receiver-count fix)))
        (is (= 1005 (poll! c)))))
    ))

(deftest test-message-rebuild-and-filter
  (testing "passende message"
    (let [one (atom 1)
          four (atom 4)
          msg1 (message/create-message "abc" "def" 1005 "payload")
          c (chan 1 (comp message/message-rebuilder
                          (filter #(message/label-match "abc" %))
                          (filter (partial is-my-message nil one four))))]
      (>!! c (message/Message->bytes msg1))
      (close! c)
      (is (message/msg= msg1 (<!! c)))))
  (testing "passiert den Filter nicht"
    (let [two (atom 2)
          four (atom 2)
          msg1 (message/create-message "abc" "def" 1005 "payload")
          c (chan 1 (comp message/message-rebuilder
                          (filter #(message/label-match "abc" %))
                          (filter (partial is-my-message nil two four))))]
      (>!! c (message/Message->bytes msg1))
      (close! c)
      (is (nil? (<!! c)))))
  )

(deftest test-msg-filter-pipeline
  (testing "simple pipeline"
    (let [c-in (chan 10)
          c-out (chan 10)]
      (pipeline 1 c-out (filter even?) c-in)
      (onto-chan c-in (range 10))
      (is (= (<!! c-out) 0))
      (is (= (<!! c-out) 2))
      (is (= (<!! c-out) 4))
      (is (= (<!! c-out) 6))
      (is (= (<!! c-out) 8))
      (close! c-in)
      (is (nil? (<!! c-out)))
     ))
  (testing "filter auf array"
    (let [msg1 (message/create-message "abc" "def" 1005 "payload")
          msg2 (message/create-message "abc" "ghi" 1004 "payload")
          one (atom 1)
          four (atom 4)]
      (is (= [msg1] (into [] (filter (partial is-my-message nil one four)) [msg1 msg2])))
    ))
  (testing "pipeline with message filter"
    (let [msg1 (message/create-message "abc" "def" 1005 "payload")
          msg2 (message/create-message "abc" "ghi" 1004 "payload")
          c-in (chan 3)
          c-out (chan 3)
          one (atom 1)
          four (atom 4)]
      (pipeline 1 c-out (filter (partial is-my-message nil one four)) c-in)
      (>!! c-in msg1)
      (>!! c-in msg2)
      (close! c-in)
      (is (= msg1 (<!! c-out)))
      (is (nil? (<!! c-out)))
      ))
  )