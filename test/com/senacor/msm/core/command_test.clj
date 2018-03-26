(ns com.senacor.msm.core.command-test
  (:require [clojure.test :refer :all]
            [com.senacor.msm.core.command :refer :all]
            [bytebuffer.buff :as bb]
            [clojure.core.async :refer [chan timeout mult >!! <!! poll! close!]]
            [com.senacor.msm.core.norm-api :as norm])
  (:import (java.nio ByteBuffer)))

(deftest test-put-fixed-header
  (testing "test put alive"
    (let [fix (bb/byte-buffer 6)]
      (is (not (nil? (put-fixed-header fix CMD_ALIVE))))
      (.flip fix)
      (is (= (byte \C) (bb/take-byte fix)))
      (is (= (byte \X) (bb/take-byte fix)))
      (is (= 1 (bb/take-byte fix)))
      (is (= 0 (bb/take-byte fix)))
      (is (= CMD_ALIVE (bb/take-byte fix)))
      (is (= 0 (.remaining fix))))))

(deftest test-command-receiver
  (testing "one single command"
    (let [session 1
          event-chan (chan 1)
          cmd-chan (chan 1)]
      (with-redefs-fn {#'norm/get-command       (fn [_] (alive session "s1" true 0 1001))}
        #(do
           (command-receiver session (mult event-chan) cmd-chan)
           (>!! event-chan {:session session :event-type :rx-object-cmd-new :node 1234})
           (let [cmd-msg (<!! cmd-chan)]
             (is (some? cmd-msg))
             (is (= 1234 (:node-id cmd-msg)))
             (is (= "s1" (:subscription cmd-msg)))
             (is (= 1001 (:msg-seq-nbr cmd-msg))))
           (Thread/sleep 100)
           )))
    )
  (testing "multiple commands"
    (let [session 1
          cmd-count (atom 4)
          event-chan (chan 1)
          cmd-chan (timeout 100)]
      (with-redefs-fn {#'norm/get-command (fn [_]
                                            (swap! cmd-count dec)
                                            (cond (pos? @cmd-count)
                                                  (alive session "s1" true 0 (- 1004 @cmd-count))
                                                  (zero? @cmd-count)
                                                  (alive session "s1" false 0 (- 1004 @cmd-count))
                                                  :else
                                                  nil))}
        #(do
           (command-receiver session (mult event-chan) cmd-chan)
           (>!! event-chan {:session session :event-type :rx-object-cmd-new :node 1234})
           (is (:active (<!! cmd-chan)))
           (>!! event-chan {:session session :event-type :rx-object-cmd-new :node 1234})
           (is (:active (<!! cmd-chan)))
           (>!! event-chan {:session session :event-type :rx-object-cmd-new :node 1234})
           (is (:active (<!! cmd-chan)))
           (>!! event-chan {:session session :event-type :rx-object-cmd-new :node 1234})
           (is (not (:active (<!! cmd-chan))))
           ))
      )
    )
  (testing "command from a different session"
    (let [cmd-chan (timeout 100)
          event-chan (chan 3)]
      (with-redefs-fn {#'norm/get-command (fn [session]
                                            (alive session (str "s" session) true 1 1000))}
        #(do
           (command-receiver 1 (mult event-chan) cmd-chan)
           (>!! event-chan {:session 1 :event-type :rx-object-cmd-new :node 1234})
           (is (= "s1234" (:subscription (<!! cmd-chan))))
           (>!! event-chan {:session 2 :event-type :rx-object-cmd-new :node 4321})
           (>!! event-chan {:session 1 :event-type :rx-object-cmd-new :node 1234})
           (is (= "s1234" (:subscription (<!! cmd-chan))))
           (is (nil? (<!! cmd-chan)))))))
  )

(deftest test-alive
  (let [fix (ByteBuffer/wrap (alive 1 "abcd" true 3 199))]
    (bb/with-buffer fix
                    (is (= (byte \C) (bb/take-byte)))
                    (is (= (byte \X) (bb/take-byte)))
                    (is (= 1 (bb/take-byte)))
                    (is (= 0 (bb/take-byte)))
                    (is (= CMD_ALIVE (bb/take-byte)))
                    (is (= 1 (bb/take-byte)))
                    (is (= 3 (bb/take-int)))
                    (is (= 199 (bb/take-long)))
                    (is (= 4 (bb/take-byte)))
                    (is (= (byte \a) (bb/take-byte)))
                    (is (= (byte \b) (bb/take-byte)))
                    (is (= (byte \c) (bb/take-byte)))
                    (is (= (byte \d) (bb/take-byte))))
    (is (= 23 (.limit fix)))
    ))

(deftest test-join
  (let [fix (ByteBuffer/wrap (join 1 "abcd" 709))]
    (bb/with-buffer fix
                    (is (= (byte \C) (bb/take-byte)))
                    (is (= (byte \X) (bb/take-byte)))
                    (is (= 1 (bb/take-byte)))
                    (is (= 0 (bb/take-byte)))
                    (is (= CMD_JOIN (bb/take-byte)))
                    (is (= 709 (bb/take-long)))
                    (is (= 4 (bb/take-byte)))
                    (is (= (byte \a) (bb/take-byte)))
                    (is (= (byte \b) (bb/take-byte)))
                    (is (= (byte \c) (bb/take-byte)))
                    (is (= (byte \d) (bb/take-byte))))
    (is (= 18 (.limit fix)))
    ))


(deftest test-command-sender
  (let [sent-msg-chan (timeout 100)]
    (with-redefs-fn {#'norm/send-command (fn [_ buf len _]
                                           (>!! sent-msg-chan [buf len]))}
      #(let [session 1
             event-chan (chan 1)
             cmd-chan (chan 2)
             fix-alive (alive session "abcd" true 5 1962)]
         (command-sender session (mult event-chan) cmd-chan)
         (>!! cmd-chan fix-alive)
         (>!! event-chan {:session session, :event-type :tx-cmd-sent})
         (let [result (<!! sent-msg-chan)]
           (is (not (nil? result)))
           (is (= 23 (second result))))
         (close! event-chan)
         (close! cmd-chan)
         ))))

(deftest test-parse-fixed-header
  (testing "well formed header"
    (let [fix (bb/byte-buffer 6)]
      (bb/with-buffer fix
                      (bb/put-byte (byte \C))
                      (bb/put-byte (byte \X))
                      (bb/put-byte 1)
                      (bb/put-byte 0)
                      (bb/put-byte CMD_ALIVE)
                      (bb/put-byte 1))
      (.flip fix)
      (is (= CMD_ALIVE (parse-fixed-header fix)))))
  (testing "bad magic"
    (let [fix (bb/byte-buffer 6)]
      (bb/with-buffer fix
                      (bb/put-byte (byte \M))
                      (bb/put-byte (byte \X))
                      (bb/put-byte 1)
                      (bb/put-byte 0)
                      (bb/put-byte CMD_ALIVE)
                      (bb/put-byte 1))
      (.flip fix)
      (is (nil? (parse-fixed-header fix)))))
  (testing "newer version"
    (let [fix (bb/byte-buffer 6)]
      (bb/with-buffer fix
                      (bb/put-byte (byte \C))
                      (bb/put-byte (byte \X))
                      (bb/put-byte 2)
                      (bb/put-byte 0)
                      (bb/put-byte CMD_ALIVE)
                      (bb/put-byte 1))
      (.flip fix)
      (is (nil? (parse-fixed-header fix)))))
  (testing "matching version"
    (let [fix (bb/byte-buffer 6)]
      (bb/with-buffer fix
                      (bb/put-byte (byte \C))
                      (bb/put-byte (byte \X))
                      (bb/put-byte 0)
                      (bb/put-byte 9)
                      (bb/put-byte CMD_ALIVE)
                      (bb/put-byte 1))
      (.flip fix)
      (is (nil? (parse-fixed-header fix)))))
  )

(deftest test-parse-command
  (testing "well formed alive command"
    (let [fix (bb/byte-buffer 256)]
      (bb/with-buffer fix
                      (bb/put-byte (byte \C))
                      (bb/put-byte (byte \X))
                      (bb/put-byte 1)
                      (bb/put-byte 0)
                      (bb/put-byte CMD_ALIVE)
                      (bb/put-byte 1)
                      (bb/put-int  5)
                      (bb/put-long 2018)
                      (bb/put-byte 2)
                      (bb/put-byte (byte \a))
                      (bb/put-byte (byte \b)))
      (.flip fix)
      (is (= {:active true,
              :cmd CMD_ALIVE,
              :msg-seq-nbr 2018,
              :session-index 5
              :subscription "ab"}
             (parse-command (.array fix))))))
  (testing "well formed join command"
    (let [fix (bb/byte-buffer 256)]
      (bb/with-buffer fix
                      (bb/put-byte (byte \C))
                      (bb/put-byte (byte \X))
                      (bb/put-byte 1)
                      (bb/put-byte 0)
                      (bb/put-byte CMD_JOIN)
                      (bb/put-long 91475)
                      (bb/put-byte 2)
                      (bb/put-byte (byte \a))
                      (bb/put-byte (byte \b)))
      (.flip fix)
      (is (= {:cmd CMD_JOIN,
              :msg-seq-nbr 91475,
              :subscription "ab"}
             (parse-command (.array fix))))))
  )