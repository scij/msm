(ns com.senacor.msm.core.stateless-test
  (:require [clojure.test :refer :all]
            [com.senacor.msm.core.stateless :refer :all]
            [clojure.core.async :refer [close! chan timeout poll! <!! >!!]]
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
           (>!! cmd-chan-in {:cmd (command/alive 2 "label" true) :node-id "remote:3456"})
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
           (>!! cmd-chan-in {:cmd (command/alive 1 "label" true) :node-id "aaa:3456"})
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
           (>!! cmd-chan-in {:cmd (command/alive 1 "label" true) :node-id "remote:3456"})
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
      (is (filter-my-messages "abc" one four fix)))
    (testing "match regex"
      (is (filter-my-messages #"ab." one four fix)))
    (testing "wrong label"
      (is (not (filter-my-messages #"xyz" one four fix))))
    (testing "wrong index"
      (is (not (filter-my-messages #"abc" two four fix))))
    (testing "only one receiver"
      (is (filter-my-messages "abc" zero one fix)))
    ))

(deftest test-msg-filter-pipeline
  ;todo test für die messagefilter pipeline fehlt
  (is false "not yet implemented"))