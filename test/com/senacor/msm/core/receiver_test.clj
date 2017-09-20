(ns com.senacor.msm.core.receiver-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [chan close! mult poll! timeout <!! >!!]]
            [com.senacor.msm.core.receiver :refer :all]
            [com.senacor.msm.core.norm-api :as norm]))

(deftest test-receive-data
  (testing "one single message"
    (let [out-chan (timeout 100)]
      (with-redefs-fn {#'norm/read-stream (fn [stream buffer size]
                                            (System/arraycopy (.getBytes "hallo") 0 buffer 0 5)
                                            5)}
        #(do
           (receive-data out-chan {:object 1})
           (is (= "hallo" (String. ^bytes (<!! out-chan))))
           )))
    )
  (testing "available data is bigger than buffer size"
    (let [out-chan (timeout 100)
          num-msgs (atom 4)]
      (with-redefs-fn {#'norm/read-stream (fn [stream buffer size]
                                            (swap! num-msgs dec)
                                            (cond (pos? @num-msgs)
                                                  (do
                                                    (System/arraycopy (.getBytes "hallo") 0 buffer 0 5)
                                                    5)
                                                  (zero? @num-msgs)
                                                  (do
                                                    (System/arraycopy (.getBytes "end bag") 0 buffer 0 7)
                                                    7)
                                                  :else 0))}
        #(do
           (receive-data out-chan {:object 1})
           (is (= "hallo" (String. ^bytes (<!! out-chan))))
           (is (= "hallo" (String. ^bytes (<!! out-chan))))
           (is (= "hallo" (String. ^bytes (<!! out-chan))))
           (is (= "end bag" (String. ^bytes (<!! out-chan))))
           (is (nil? (<!! out-chan)))
           )
        )))
  )

(deftest test-receiver-handler
  (testing "handle immediate close without an event"
    (let [session 1
          event-chan (chan 1)
          out-chan (chan)]
      (with-redefs-fn {#'close-receiver (fn [session out-chan]
                                        (>!! out-chan :stopped))
                       #'receive-data   (fn [_ _])}
        #(do
           (receiver-handler session (mult event-chan) out-chan)
           (close! event-chan)
           (is (nil? (poll! out-chan)))
           ))))
  (testing "handle closing instance"
    (let [session 1
          event-chan (chan 1)
          out-chan (chan)]
      (with-redefs-fn {#'close-receiver (fn [session out-chan]
                                        (>!! out-chan :stopped))
                       #'receive-data   (fn [_ _])}
        #(do
           (receiver-handler session (mult event-chan) out-chan)
           (>!! event-chan {:session session :event-type :event-invalid})
           (is (nil? (<!! out-chan)))
           ))))
  (testing "handle closing stream (aborted)"
    (let [session 1
          event-chan (chan 1)
          out-chan (timeout 100)]
      (with-redefs-fn {#'receive-data   (fn [_ _])}
        #(do
           (receiver-handler session (mult event-chan) out-chan)
           (>!! event-chan {:session session :event-type :rx-object-aborted})
           (is (nil? (poll! out-chan)))
           ))))
  (testing "handle closing stream (complete)"
    (let [session 1
          event-chan (chan 1)
          out-chan (timeout 100)]
      (with-redefs-fn {#'receive-data   (fn [_ _])}
        #(do
           (receiver-handler session (mult event-chan) out-chan)
           (>!! event-chan {:session session :event-type :rx-object-completed})
           (is (nil? (poll! out-chan)))
           ))))
  (testing "handle unknown event type"
    (let [session 1
          event-chan (chan 1)
          out-chan (timeout 100)]
      (with-redefs-fn {#'close-receiver (fn [_ _])
                       #'receive-data   (fn [_ _]
                                        (>!! out-chan :received))}
        #(do
           (receiver-handler session (mult event-chan) out-chan)
           (>!! event-chan {:session session :event-type :unknown})
           (is (= nil (poll! out-chan)))
           (>!! event-chan {:session session :event-type :rx-object-completed})
           (<!! out-chan)
           ))))
  (testing "handle different session for close"
    (let [session 1
          event-chan (chan 1)
          out-chan (timeout 100)]
      (with-redefs-fn {#'receive-data   (fn [_ _])}
        #(do
           (receiver-handler session (mult event-chan) out-chan)
           (>!! event-chan {:session (inc session) :event-type :rx-object-completed})
           (is (nil? (poll! out-chan)))
           ))))
  (testing "receive some data"
    (let [session 1
          event-chan (chan 1)
          out-chan (chan 1)]
      (with-redefs-fn {#'close-receiver (fn [_ _])
                       #'receive-data   (fn [_ _]
                                        (>!! out-chan :data))}
        #(do
           (receiver-handler session (mult event-chan) out-chan)
           (>!! event-chan {:session session :event-type :rx-object-updated})
           (is (= :data (<!! out-chan)))
           (close! event-chan)
           ))))
  )
