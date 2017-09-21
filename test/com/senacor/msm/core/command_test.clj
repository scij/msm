(ns com.senacor.msm.core.command-test
  (:require [clojure.test :refer :all]
            [com.senacor.msm.core.command :refer :all]
            [bytebuffer.buff :as bb]
            [clojure.core.async :refer [chan timeout mult >!! <!! poll!]]
            [com.senacor.msm.core.norm-api :as norm])
  (:import (java.nio Buffer ByteBuffer)))

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
          cmd-chan (timeout 100)]
      (with-redefs-fn {#'norm/get-command       (fn [_] (.getBytes "hallo"))
                       #'norm/get-local-node-id (fn [_] 1)}
        #(do
           (command-receiver session (mult event-chan) cmd-chan)
           (>!! event-chan {:session session :event-type :rx-object-cmd-new})
           (is (= "hallo" (String. ^bytes (<!! cmd-chan))))
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
                                                  (.getBytes "hallo")
                                                  (zero? @cmd-count)
                                                  (.getBytes "end bag")
                                                  :else
                                                  nil))
                       #'norm/get-local-node-id (fn [_] 1)}
        #(do
           (command-receiver session (mult event-chan) cmd-chan)
           (>!! event-chan {:session session :event-type :rx-object-cmd-new})
           (is (= "hallo" (String. ^bytes (<!! cmd-chan))))
           (>!! event-chan {:session session :event-type :rx-object-cmd-new})
           (is (= "hallo" (String. ^bytes (<!! cmd-chan))))
           (>!! event-chan {:session session :event-type :rx-object-cmd-new})
           (is (= "hallo" (String. ^bytes (<!! cmd-chan))))
           (>!! event-chan {:session session :event-type :rx-object-cmd-new})
           (is (= "end bag" (String. ^bytes (<!! cmd-chan))))
           ))
      )
    )
  )

(deftest test-alive
  (with-redefs-fn {#'norm/get-local-node-id (fn [_])
                   #'norm/get-node-name (fn [_] "efgh")}
    #(let [fix (ByteBuffer/wrap (alive 1 "abcd" true))]
       (bb/with-buffer fix
                       (is (= (byte \C) (bb/take-byte)))
                       (is (= (byte \X) (bb/take-byte)))
                       (is (= 1 (bb/take-byte)))
                       (is (= 0 (bb/take-byte)))
                       (is (= CMD_ALIVE (bb/take-byte)))
                       (is (= 1 (bb/take-byte)))
                       (is (= 4 (bb/take-byte)))
                       (is (= (byte \e) (bb/take-byte)))
                       (is (= (byte \f) (bb/take-byte)))
                       (is (= (byte \g) (bb/take-byte)))
                       (is (= (byte \h) (bb/take-byte)))
                       (is (= 4 (bb/take-byte)))
                       (is (= (byte \a) (bb/take-byte)))
                       (is (= (byte \b) (bb/take-byte)))
                       (is (= (byte \c) (bb/take-byte)))
                       (is (= (byte \d) (bb/take-byte))))
       (is (= 16 (.limit fix)))
       )))


(deftest test-command-sender
  (let [sent-msg-chan (timeout 10)]
    (with-redefs-fn {#'norm/send-command (fn [_ buf len _]
                                           (>!! sent-msg-chan [buf len]))
                     #'norm/get-local-node-id (fn [_])
                     #'norm/get-node-name (fn [_] "efgh")}
      #(let [session 1
             event-chan (chan 1)
             cmd-chan (chan 2)
             fix-alive (alive session "abcd" true)]
         (command-sender session (mult event-chan) cmd-chan)
         (>!! cmd-chan fix-alive)
         (>!! event-chan {:session session, :event-type :tx-cmd-sent})
         (let [result (<!! sent-msg-chan)]
           (is (not (nil? result)))
           (is (= 16 (second result))))
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
  (testing "well formed command"
    (let [fix (bb/byte-buffer 256)]
      (bb/with-buffer fix
                      (bb/put-byte (byte \C))
                      (bb/put-byte (byte \X))
                      (bb/put-byte 1)
                      (bb/put-byte 0)
                      (bb/put-byte CMD_ALIVE)
                      (bb/put-byte 1)
                      (bb/put-byte 2)
                      (bb/put-byte (byte \a))
                      (bb/put-byte (byte \b))
                      (bb/put-byte 2)
                      (bb/put-byte (byte \e))
                      (bb/put-byte (byte \f)))
      (.flip fix)
      (is (= {:active true,
              :cmd CMD_ALIVE,
              :node-id "ab",
              :subscription "ef"}
             (parse-command (.array fix))))))
  )