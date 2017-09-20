(ns com.senacor.msm.core.sender-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [mult chan >!! close! timeout poll! <!! onto-chan]]
            [com.senacor.msm.core.sender :refer :all]
            [com.senacor.msm.core.norm-api :as norm]
            [clojure.tools.logging :as log]
            [com.senacor.msm.core.monitor :as mon]))

(deftest test-create-sender
  (let [session 1
        instance-id 1]
    (testing "eine kurze Message"
      (let [event-chan (chan 1)
            in-chan (chan 1)
            test-chan (timeout 1000)]
        (with-redefs-fn {#'norm/write-stream (fn [stream b-arr b-offs b-len] (>!! test-chan b-arr) b-len),
                         #'norm/start-sender (fn [_ _ _ _ _ _]),
                         #'norm/open-stream  (fn [_ _] 99),
                         #'norm/mark-eom     (fn [_])
                         #'mon/record-bytes-sent (fn [_ _])
                         #'stop-sender (fn [_ _ _] (>!! test-chan true))}
          #(do
             (create-sender session instance-id (mult event-chan) in-chan 128)
             (>!! in-chan (.getBytes "hallo"))
             (is (= "hallo" (String. ^bytes (<!! test-chan))))
             (close! in-chan)
             (close! event-chan)
             (<!! test-chan)))
        ))
    (testing "viele kurze Messages"
      (let [event-chan (chan 1)
            in-chan (chan 10)
            test-chan (timeout 1000)
            send-count (atom 0)]
        (with-redefs-fn {#'norm/write-stream (fn [_ b-arr b-offs b-len]
                                               (is (= b-len 8))
                                               (is (= b-offs 0))
                                               (swap! send-count inc)
                                               b-len)
                         #'norm/start-sender (fn [_ _ _ _ _ _])
                         #'norm/open-stream  (fn [_ _] 99)
                         #'norm/mark-eom     (fn [_])
                         #'mon/record-bytes-sent (fn [_ _])
                         #'stop-sender       (fn [_ _ _] (>!! test-chan true))}
          #(do
             (create-sender session instance-id (mult event-chan) in-chan 128)
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
            test-chan (timeout 1000)]
        (with-redefs-fn {#'norm/write-stream (fn [stream b-arr b-offs b-len]
                                               (log/tracef "write-stream %d %d" b-offs b-len)
                                               (>!! event-chan {:session 1, :event-type :tx-queue-vacancy})
                                               (swap! send-count inc)
                                               8),
                         #'norm/start-sender (fn [_ _ _ _ _ _]),
                         #'norm/open-stream  (fn [_ _] 99),
                         #'norm/mark-eom     (fn [_]),
                         #'mon/record-bytes-sent (fn [_ _])
                         #'stop-sender       (fn [_ _ _] (>!! test-chan true))}
          #(do
             (create-sender session instance-id (mult event-chan) in-chan 128)
             (>!! in-chan (.getBytes (apply str (repeat 20 "abcdefg "))))
             (close! in-chan)
             (close! event-chan)
             (<!! test-chan)
             (is (= 20 @send-count))
             ))
        ))
    ))