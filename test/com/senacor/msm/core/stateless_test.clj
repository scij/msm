(ns com.senacor.msm.core.stateless-test
  (:require [clojure.test :refer :all]
            [com.senacor.msm.core.stateless :refer :all]
            [clojure.core.async :refer [onto-chan close! chan timeout pipeline poll! <!! >!!]]
            [com.senacor.msm.core.command :as command]
            [com.senacor.msm.core.norm-api :as norm]
            [com.senacor.msm.core.message :as message]
            [com.senacor.msm.core.monitor :as monitor])
  (:import (java.util.concurrent Future)))

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
           (>!! cmd-chan-in {:cmd (command/alive 2 "label" true 199) :node-id "remote:3456"})
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
           (>!! cmd-chan-in {:cmd (command/alive 1 "label" true 200) :node-id "aaa:3456"})
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
           (>!! cmd-chan-in {:cmd (command/alive 1 "label" true 200) :node-id "remote:3456"})
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
  (let [fix (message/create-message "abc" "def" "payload")
        zero (atom 0)
        two (atom 2)
        four (atom 4)
        one (atom 1)]
    (testing "match"
      (is (is-my-message "abc" one four fix)))
    (testing "match regex"
      (is (is-my-message #"ab." one four fix)))
    (testing "wrong label"
      (is (not (is-my-message #"xyz" one four fix))))
    (testing "wrong index"
      (is (not (is-my-message #"abc" two four fix))))
    (testing "only one receiver"
      (is (is-my-message "abc" zero one fix)))
    ))

(deftest test-message-rebuild-and-filter
  (testing "passende message"
    (let [one (atom 1)
          four (atom 4)
          msg1 (message/create-message "abc" "def" "payload")
          c (chan 1 (comp message/message-rebuilder
                          (filter (partial is-my-message "abc" one four))))]
      (>!! c (message/Message->bytes msg1))
      (is (message/msg= msg1 (<!! c)))))
  (testing "passiert den Filter nicht"
    (let [two (atom 2)
          four (atom 2)
          msg1 (message/create-message "abc" "def" "payload")
          c (chan 1 (comp message/message-rebuilder
                          (filter (partial is-my-message "abc" two four))))]
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
    (let [msg1 (message/create-message "abc" "def" "payload")
          msg2 (message/create-message "abc" "ghi" "payload")
          one (atom 1)
          four (atom 4)]
      (is (= [msg1] (into [] (filter (partial is-my-message "abc" one four)) [msg1 msg2])))
    ))
  (testing "pipeline with message filter"
    (let [msg1 (message/create-message "abc" "def" "payload")
          msg2 (message/create-message "abc" "ghi" "payload")
          c-in (chan 3)
          c-out (chan 3)
          one (atom 1)
          four (atom 4)]
      (pipeline 1 c-out (filter (partial is-my-message "abc" one four)) c-in)
      (>!! c-in msg1)
      (>!! c-in msg2)
      (close! c-in)
      (is (= msg1 (<!! c-out)))
      (is (nil? (<!! c-out)))
      ))
  )