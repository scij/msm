(ns com.senacor.msm.core.stateless-test
  (:require [clojure.test :refer :all]
            [com.senacor.msm.core.stateless :refer :all]
            [clojure.core.async :refer [mult go go-loop onto-chan close! chan onto-chan timeout pipe pipeline poll!
                                        sliding-buffer <! >! <!! >!!]]
            [com.senacor.msm.core.command :as command]
            [com.senacor.msm.core.norm-api :as norm]
            [com.senacor.msm.core.message :as message]
            [com.senacor.msm.core.monitor :as monitor]
            [com.senacor.msm.core.receiver :as receiver]
            [clojure.tools.logging :as log]
            [me.raynes.moments :as moments]
            [com.senacor.msm.core.util :as util]))

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
    (let [fix (sorted-map 4711 {:expires 500, :subscription "smy"})]
      (is (= 0 (find-my-index fix 4711)))))
  (testing "two entries, my session 2nd"
    (let [fix (sorted-map 4711 {:expires 500, :subscription "smy"}
                          4710 {:expires 600, :subscription "s600"})]
      (is (= 1 (find-my-index fix 4711)))))
  (testing "two entries, my session 1st"
    (let [fix (sorted-map 4711 {:expires 500, :subscription "smy"}
                          4712 {:expires 600, :subscription "s600"})]
      (is (= 0 (find-my-index fix 4711)))))
  (testing "three entries, my session in the middle"
    (let [fix (sorted-map 4711 {:expires 500, :subscription "smy"}
                          4712 {:expires 600, :subscription "s600"}
                          4710 {:expires 300, :subscription "s300"})]
      (is (= 1 (find-my-index fix 4711))))))

(deftest test-handle-receiver-status
  (with-redefs-fn {#'norm/get-local-node-id (fn [sess] sess),
                   #'norm/get-node-id (fn [node] node),
                   #'monitor/record-number-of-sl-receivers (fn [_ _])
                   #'monitor/record-sl-receivers (fn [_ _])}
    #(do
       (testing "add another receiver"
         (log/trace "### Enter" *testing-contexts*)
         (let [session 1
               my-session-index (atom 0)
               receiver-count (atom 1)
               session-receivers (atom {session {:expires Long/MAX_VALUE}})
               cmd-chan-in (chan 1)
               task (handle-receiver-status 1 "label" cmd-chan-in session-receivers my-session-index receiver-count)]
           (Thread/sleep 10)
           (is (= 1 @receiver-count))
           (>!! cmd-chan-in {:cmd command/CMD_ALIVE,
                             :active true,
                             :subscription "label"
                             :session-index 1,
                             :msg-seq-nbr 1000,
                             :node-id 2})
           (Thread/sleep 100) ; todo add a better way to synchronize
           (receiver-status-housekeeping session session-receivers receiver-count my-session-index)
           (is (= 0 @my-session-index))
           (is (= 2 @receiver-count))
           (close! cmd-chan-in)
           ))
       (testing "add another receiver with a lower session id"
         (log/trace "### Enter" *testing-contexts*)
         (let [my-session-index (atom 0)
               receiver-count (atom 1)
               session 2
               session-receivers (atom {session {:expires Long/MAX_VALUE}})
               cmd-chan-in (chan 1)
               task (handle-receiver-status session "label" cmd-chan-in session-receivers my-session-index receiver-count)]
           (Thread/sleep 10)
           (is (= 1 @receiver-count))
           (>!! cmd-chan-in {:cmd command/CMD_ALIVE,
                             :active true,
                             :subscription "label",
                             :session-index @my-session-index,
                             :msg-seq-nbr 1000,
                             :node-id 1})
           (Thread/sleep 100) ; todo add a better way to synchronize
           (receiver-status-housekeeping session session-receivers receiver-count my-session-index)
           (is (= 1 @my-session-index))
           (is (= 2 @receiver-count))
           (close! cmd-chan-in)
           ))
       (testing "expire another receiver"
         (log/trace "### Enter" *testing-contexts*)
         (let [my-session-index (atom 0)
               receiver-count (atom 1)
               session 2
               session-receivers (atom {session {:expires Long/MAX_VALUE}})
               cmd-chan-in (chan 1)
               task (handle-receiver-status session "label" cmd-chan-in session-receivers my-session-index receiver-count)]
           (>!! cmd-chan-in {:cmd command/CMD_ALIVE,
                             :subscription "label",
                             :active true,
                             :session-id 0,
                             :msg-seq-nbr 1000,
                             :node-id 1})
           (Thread/sleep 100) ; todo add a better way to synchronize
           (receiver-status-housekeeping session session-receivers receiver-count my-session-index)
           (is (= 2 @receiver-count))
           (is (= 1 @my-session-index))
           (Thread/sleep (+ expiry-threshold 10))
           (receiver-status-housekeeping session session-receivers receiver-count my-session-index)
           (is (= 1 @receiver-count))
           (is (= 0 @my-session-index))
           ))
       )))

(deftest test-filter-my-messages
  (let [fix (message/create-message "abc" "def" 4 "payload")
        zero (atom 0)
        a-nil (atom nil)
        two (atom 2)
        four (atom 4)
        one (atom 1)]
    (testing "match"
      (is (is-my-message zero four fix)))
    (testing "wrong index, not my message"
      (is (not (is-my-message two four fix))))
    (testing "only one receiver"
      (is (is-my-message zero one fix)))
    (testing "index not yet set"
      (is (not (is-my-message a-nil one fix)))
      ))
  )

(deftest test-message-filter
  (let [ts (atom 0)]
    (with-redefs-fn
      {#'util/now-ts (fn [] (swap! ts inc))}
      (fn []
        (let [fix-msgs (map #(message/create-message "s1" (str "co" %) (+ 1000 %) (str "Payload " %))
                            (range 1 500))
              fix-b-arrs (map message/Message->bytes fix-msgs)
              fix-one-b-arr (reduce util/cat-byte-array (map message/Message->bytes fix-msgs))
              out-chan (chan (sliding-buffer 1))
              a-zero (atom 0)
              a-one (atom 1)
              a-two (atom 2)]
          (testing "message filter test - one consumber"
            (println "*** At" *testing-contexts*)
            (reset! ts -1)
            (is (= (into [] (take-last 99 fix-msgs))
                   (into []
                         (filter-fn-builder 1 "s1" out-chan a-zero a-one)
                         fix-b-arrs))))
          (testing "message filter test - two consumers"
            (println "*** At" *testing-contexts*)
            (reset! ts -1)
            (is (= (filter #(even? (:msg-seq-nbr %)) (take-last 98 fix-msgs))
                   (into []
                         (filter-fn-builder 1 "s1" out-chan a-zero a-two)
                         fix-b-arrs))))
          (testing "message filter test - one consumer and one byte array"
            (println "*** At" *testing-contexts*)
            (reset! ts -1)
            (is (= (into [] (take-last 99 fix-msgs))
                   (into []
                         (filter-fn-builder 1 "s1" out-chan a-zero a-one)
                         [fix-one-b-arr]))))
          (testing "Join timer expiry without a message"
            (println "*** At" *testing-contexts*)
            (with-redefs-fn
              {#'join-wait-timestamp (fn [] 0)}
              #(do
                 (reset! ts 0)
                 (is (= (into [] (take-last 499 fix-msgs))
                        (into []
                              (filter-fn-builder 1 "s1" out-chan a-zero a-one)
                              fix-b-arrs))))))
          ))
      )))

(deftest test-join-filter
  (testing "All should be filtered because we stop sending before the threshold is reached"
    (with-redefs-fn
      {#'util/now-ts (fn [] 1000)}
      (fn []
        (let [fix-msgs0 (map #(message/create-message "s1" (str "co" %) (+ 5 %) (str "Payload " %))
                             (range 1 50))
              now 1000
              fix-msgs1 (map #(assoc % :receive-ts (+ now (:msg-seq-nbr %)))
                             fix-msgs0)]
          (is (= [] (into [] (join-filter (partial command/join 1 "s1") nil) fix-msgs1)))
          ))))
  (testing "First 100 should be filtered and the rest should pass"
    (with-redefs-fn
      {#'util/now-ts (fn [] 1000)}
      (fn []
        (let [fix-msgs0 (map #(message/create-message "s1" (str "co" %) % (str "Payload " %))
                             (range 1 (* 5 alive-interval)))
              now 1000
              fix-msgs1 (map #(assoc % :receive-ts (+ now (:msg-seq-nbr %)))
                             fix-msgs0)]
          (is (= (take-last 98 fix-msgs1)
                 (into [] (join-filter (partial command/join 1 "s1") nil) fix-msgs1)
                 ))))))
  )

(deftest test-stateless-session-handler
  (testing "sending commands"
    (log/trace "*** Enter" *testing-contexts*)
    (let [test-chan (chan 4)]
      (with-redefs-fn {#'norm/start-sender (fn [_ _ _ _ _ _]),
                       #'norm/get-local-node-id   (fn [session] 1),
                       #'command/command-sender (fn [_ _ cmd-chan]
                                                  (pipe cmd-chan test-chan)),
                       #'moments/schedule-every (fn [_ _ f]
                                                  (doseq [i (range 3)]
                                                    (f)))
                       }
        #(let [cmd-chan (chan 4)
               a-my-session-index (atom 0)
               a-my-session-last-msg (atom 99)
               fix {:active        true
                    :cmd           1
                    :msg-seq-nbr   99
                    :session-index 0
                    :subscription  "s1"}]
           (is (nil? (send-status-messages 1 "s1" nil cmd-chan a-my-session-index a-my-session-last-msg)))
           (is (= fix (command/parse-command (<!! test-chan))))
           (is (= fix (command/parse-command (<!! test-chan))))
           (is (= fix (command/parse-command (<!! test-chan))))
           ))))
  (testing "receiving commands"
    (log/trace "*** Enter" *testing-contexts*)
    (let [ctl-chan (chan 1)]
      (with-redefs-fn {#'monitor/record-sl-receivers (fn [_ _]),
                       #'monitor/record-number-of-sl-receivers (fn [_ _]),
                       #'norm/get-local-node-id (fn [n] n),
                       #'norm/get-node-id (fn [n] n)
                       #'moments/schedule-every (fn [_ interval f]
                                                  (f)
                                                  (Thread/sleep interval)
                                                  (f)
                                                  :scheduled),
                       #'command/command-receiver (fn [_ _ cc]
                                                    (>!! cc {:subscription "s1",
                                                            :node-id 2})
                                                    (>!! cc {:subscription "s1",
                                                            :node-id 3}))}
        (fn []
          (let [cmd-chan (chan 4)
                session 1
               a-session-receivers (atom (sorted-map session {:expires Long/MAX_VALUE,
                                                                 :subscription "s1"}))
               a-my-session-index (atom 0)
               a-receiver-count (atom 1)]
           (is (= :scheduled (receive-status-messages session "s1" nil a-session-receivers a-my-session-index a-receiver-count)))
           (is (= 3 @a-receiver-count))
           )))))
  (testing "receiving data"
    (log/trace "*** Enter" *testing-contexts*)
    (let [session 1
          subscription "s1"
          event-chan (chan 1)
          msg-chan (chan 10)
          cmd-chan-out (chan 1)
          ts (atom 0)
          a-my-session-index (atom 0)
          a-receiver-count (atom 2)]
      (with-redefs-fn {#'util/now-ts (fn [] (swap! ts inc)),
                       #'receiver/create-receiver (fn [_ _ msg-chan filter-fn]
                                                    (log/trace "Start pseudo receiver")
                                                    (let [start 1000
                                                          in-chan (chan 10 filter-fn)]
                                                      (pipe in-chan msg-chan)
                                                      (log/trace "start producing messages")
                                                      (go-loop [t start
                                                                seq 0]
                                                        (>! in-chan (message/Message->bytes (message/create-message subscription "corr1" seq (str "Msg " seq))))
                                                        (when (< t (+ start alive-interval alive-interval))
                                                          (recur (inc t) (inc seq))))
                                                      ))}
        (fn []
          (is (receive-data session subscription event-chan msg-chan cmd-chan-out a-my-session-index a-receiver-count))
          (is (= 200 (:msg-seq-nbr (<!! msg-chan))))
          (is (= {:cmd 2,
                  :msg-seq-nbr 199,
                  :subscription "s1"} (command/parse-command(<!! cmd-chan-out))))
          ))))
  )

(deftest test-is-my-message
  (let [fix (message/create-message "abc" "def" 1005 "payload")
        a-my-index1 (atom 1)
        a-my-index2 (atom 0)
        a-receiver-count (atom 4)]
    (testing "is my message"
      (is (is-my-message a-my-index1 a-receiver-count fix)))
    (testing "is not my message"
      (is (not (is-my-message a-my-index2 a-receiver-count fix))))
    (testing "not initialized"
      (is (not (is-my-message (atom nil) a-receiver-count fix))))
    ))

(deftest test-message-rebuild-and-filter
  (testing "passende message"
    (let [one (atom 1)
          four (atom 4)
          msg1 (message/create-message "abc" "def" 1005 "payload")
          c (chan 1 (comp message/message-rebuilder
                          (filter #(message/label-match "abc" %))
                          (filter (partial is-my-message one four))))]
      (>!! c (message/Message->bytes msg1))
      (close! c)
      (is (message/msg= msg1 (<!! c)))))
  (testing "passiert den Filter nicht"
    (let [two (atom 2)
          four (atom 2)
          msg1 (message/create-message "abc" "def" 1005 "payload")
          c (chan 1 (comp message/message-rebuilder
                          (filter #(message/label-match "abc" %))
                          (filter (partial is-my-message two four))))]
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
      (is (= [msg1] (into [] (filter (partial is-my-message one four)) [msg1 msg2])))
    ))
  (testing "pipeline with message filter"
    (let [msg1 (message/create-message "abc" "def" 1005 "payload")
          msg2 (message/create-message "abc" "ghi" 1004 "payload")
          c-in (chan 3)
          c-out (chan 3)
          one (atom 1)
          four (atom 4)]
      (pipeline 1 c-out (filter (partial is-my-message one four)) c-in)
      (>!! c-in msg1)
      (>!! c-in msg2)
      (close! c-in)
      (is (= msg1 (<!! c-out)))
      (is (nil? (<!! c-out)))
      ))
  )