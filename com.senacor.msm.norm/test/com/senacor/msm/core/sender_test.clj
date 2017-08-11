(ns com.senacor.msm.core.sender-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [mult chan >!! close! timeout poll! <!! onto-chan]]
            [com.senacor.msm.core.sender :refer :all]
            [com.senacor.msm.core.norm-api :as norm]
            [clojure.tools.logging :as log]))

(deftest test-wait-for-events
  (testing "gleich ein treffer"
    (let [event {:session 1 :event-type 1}
          test-c (chan 1)]
      (>!! test-c event)
      (close! test-c)
      (is (= event (wait-for-events test-c 1 #{1})))))
  (testing "gleich ein treffer aus vielen"
    (let [event {:session 1 :event-type 1}
          test-c (chan 1)]
      (>!! test-c event)
      (close! test-c)
      (is (= event (wait-for-events test-c 1 #{1 2})))))
  (testing "gleich geschlossen"
    (let [test-c (chan 1)]
      (close! test-c)
      (is (nil? (wait-for-events test-c 1 #{1})))))
  (testing "nicht die passende session"
    (let [event {:session 1 :event-type 1}
          test-c (chan 1)]
      (>!! test-c event)
      (close! test-c)
      (is (nil? (wait-for-events test-c 2 #{1})))))
  (testing "nicht der passende event"
    (let [event {:session 1 :event-type 1}
          test-c (chan 1)]
      (>!! test-c event)
      (close! test-c)
      (is (nil? (wait-for-events test-c 1 #{2})))))
  (testing "nicht der passende event aus mehreren"
    (let [event {:session 1 :event-type 1}
          test-c (chan 1)]
      (>!! test-c event)
      (close! test-c)
      (is (nil? (wait-for-events test-c 1 #{2 3})))))
  (testing "beides passt nicht"
    (let [event {:session 1 :event-type 1}
          test-c (chan 1)]
      (>!! test-c event)
      (close! test-c)
      (is (nil? (wait-for-events test-c 2 #{2})))))
  )

(deftest test-command-handler
  (let [session 1
        event-chan (chan 1)
        test-chan (chan 1)
        cmd-chan (chan 1)]
    (with-redefs-fn {#'norm/send-command (fn [session cmd cmd-len _] (>!! test-chan cmd))}
      #(do
         (is (command-handler session (mult event-chan) cmd-chan))
         (>!! cmd-chan "hallo")
         (>!! event-chan {:event-type :tx-cmd-sent :session 1})
         (is (= "hallo" (<!! test-chan)))
         (close! test-chan)
         (close! event-chan)
         (close! cmd-chan)
         ))))

(deftest test-create-sender
  (let [session 1
        instance-id 1]
    (testing "eine kurze Message"
      (let [event-chan (chan 1)
            in-chan (chan 1)
            cmd-chan (chan 1)
            test-chan (timeout 1000)]
        (with-redefs-fn {#'norm/write-stream (fn [stream b-arr b-offs b-len] (>!! test-chan b-arr) b-len),
                         #'norm/start-sender (fn [_ _ _ _ _ _]),
                         #'norm/open-stream  (fn [_ _] 99),
                         #'stop-sender       (fn [_ _ _](>!! test-chan true))}
          #(do
             (create-sender session instance-id (mult event-chan) in-chan cmd-chan 128)
             (>!! in-chan (.getBytes "hallo"))
             (is (= "hallo" (String. ^bytes (<!! test-chan))))
             (close! in-chan)
             (close! event-chan)
             (<!! test-chan)))
        ))
    (testing "viele kurze Messages"
      (let [event-chan (chan 1)
            in-chan (chan 10)
            cmd-chan (chan 1)
            test-chan (timeout 1000)
            send-count (atom 0)]
        (with-redefs-fn {#'norm/write-stream (fn [_ b-arr b-offs b-len]
                                               (is (= b-len 8))
                                               (is (= b-offs 0))
                                               (swap! send-count inc)
                                               b-len)
                         #'norm/start-sender (fn [_ _ _ _ _ _])
                         #'norm/open-stream  (fn [_ _] 99)
                         #'stop-sender       (fn [_ _ _] (>!! test-chan true))}
          #(do
             (create-sender session instance-id (mult event-chan) in-chan cmd-chan 128)
             (doseq [i (range 20)]
               (>!! in-chan (.getBytes "12345678")))
             (close! in-chan)
             (close! event-chan)
             (<!! test-chan)
             (is (= @send-count 20))))
        ))
    (testing "eine Message, die nicht in einem Stück versendet wird"
      (let [send-count (atom 0)
            event-chan (chan 1)
            in-chan (chan 1)
            cmd-chan (chan 1)
            test-chan (timeout 1000)]
        (with-redefs-fn {#'norm/write-stream (fn [stream b-arr b-offs b-len]
                                               (log/tracef "write-stream %d %d" b-offs b-len)
                                               (>!! event-chan {:session 1, :event-type :tx-queue-vacancy})
                                               (swap! send-count inc)
                                               8),
                         #'norm/start-sender (fn [_ _ _ _ _ _]),
                         #'norm/open-stream  (fn [_ _] 99),
                         #'stop-sender  (fn [_ _ _] (>!! test-chan true))}
          #(do
             (create-sender session instance-id (mult event-chan) in-chan cmd-chan 128)
             (>!! in-chan (.getBytes (apply str (repeat 20 "abcdefg "))))
             (close! in-chan)
             (close! event-chan)
             (<!! test-chan)
             (is (= 20 @send-count))
             ))
        ))
    ))