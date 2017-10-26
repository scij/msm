(ns com.senacor.msm.core.receiver-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [chan close! mult poll! mix admix timeout <!! >!!]]
            [com.senacor.msm.core.receiver :refer :all]
            [com.senacor.msm.core.norm-api :as norm]
            [com.senacor.msm.core.monitor :as monitor]
            [com.senacor.msm.core.monitor :as mon]
            [clojure.tools.logging :as log]))

(deftest test-receive-data
  (testing "one single message"
    (let [out-chan (timeout 100)]
      (with-redefs-fn {#'norm/read-stream (fn [stream buffer size]
                                            (System/arraycopy (.getBytes "hallo") 0 buffer 0 5)
                                            5)
                       #'monitor/record-bytes-received (fn [_ _])}
        #(do
           (receive-data 1 1 out-chan)
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
                                                  :else 0))
                       #'monitor/record-bytes-received (fn [_ _])}
        #(do
           (receive-data 1 1 out-chan)
           (is (= "hallo" (String. ^bytes (<!! out-chan))))
           (is (= "hallo" (String. ^bytes (<!! out-chan))))
           (is (= "hallo" (String. ^bytes (<!! out-chan))))
           (is (= "end bag" (String. ^bytes (<!! out-chan))))
           (is (nil? (<!! out-chan)))
           )
        )))
  )

(deftest test-stream-handler
  (testing "one message one matching event"
    (println "At" *testing-contexts*)
    (let [session 1
          stream :stream
          event-chan (chan 2)
          out-chan (chan 2)
          out-mix (mix out-chan)
          stream-chan (chan 2)]
      (with-redefs-fn {#'receive-data (fn [_ _ c] (>!! c "hallo")),
                       #'norm/seek-message-start (fn [_] true)}
        #(do
           (admix out-mix stream-chan)
           (stream-handler session stream (mult event-chan) out-mix stream-chan)
           (>!! event-chan {:session session :object stream :event-type :rx-object-updated})
           (>!! event-chan {:session session :object stream :event-type :rx-object-completed})
           (close! event-chan)
           (is (= "hallo" (<!! out-chan)))
           (is (nil? (poll! out-chan)))))))
  (testing "one message, event session not matching"
    (println "At" *testing-contexts*)
    (let [session 1
          stream :stream
          event-chan (chan 2)
          out-chan (chan 2)
          out-mix (mix out-chan)
          stream-chan (chan 2)]
      (with-redefs-fn {#'receive-data (fn [_ _ c] (>!! c "hallo")),
                       #'norm/seek-message-start (fn [_] true)}
        #(do
            (admix out-mix stream-chan)
            (stream-handler session stream (mult event-chan) out-mix stream-chan)
            (>!! event-chan {:session 0, :object stream, :event-type :rx-object-updated})
            (is (nil? (poll! out-chan)))
            (>!! event-chan {:session session, :object stream, :event-type :rx-object-completed})
            (close! event-chan)))))
  (testing "one message, event stream not matching"
    (println "At" *testing-contexts*)
    (let [session 1
          stream :stream
          event-chan (chan 2)
          out-chan (chan 2)
          out-mix (mix out-chan)
          stream-chan (chan 2)]
      (with-redefs-fn {#'receive-data (fn [_ _ c] (>!! c "hallo")),
                       #'norm/seek-message-start (fn [_] true)}
        #(do
            (admix out-mix stream-chan)
            (stream-handler session stream (mult event-chan) out-mix stream-chan)
            (>!! event-chan {:session session, :object 0, :event-type :rx-object-updated})
            (is (nil? (poll! out-chan)))
            (>!! event-chan {:session session, :object stream, :event-type :rx-object-completed})
            (close! event-chan)))))
  )

(def bytearray-fx
  (map (fn [^bytes b] (String. b))))

(def nil-fx
  (map (fn [x] x)))

(deftest test-receiver-handler
  (testing "handle immediate close without an event"
    (println "At" *testing-contexts*)
    (let [session 1
          event-chan (chan 1)
          out-chan (chan)]
      (with-redefs-fn {#'close-receiver (fn [session out-chan]
                                        (>!! out-chan :stopped))
                       #'receive-data   (fn [_ _ _])}
        #(do
           (receiver-handler session (mult event-chan) out-chan bytearray-fx)
           (close! event-chan)
           (is (nil? (poll! out-chan)))
           ))))
  (testing "handle closing instance"
    (println "At" *testing-contexts*)
    (let [session 1
          event-chan (chan 1)
          out-chan (chan)]
      (with-redefs-fn {#'close-receiver (fn [session out-chan]
                                        (>!! out-chan :stopped))
                       #'receive-data   (fn [_ _ _])}
        #(do
           (receiver-handler session (mult event-chan) out-chan bytearray-fx)
           (>!! event-chan {:session session :event-type :event-invalid})
           (is (nil? (<!! out-chan)))
           ))))
  (testing "handle closing stream (aborted)"
    (println "At" *testing-contexts*)
    (let [session 1
          event-chan (chan 1)
          out-chan (timeout 100)]
      (with-redefs-fn {#'receive-data   (fn [_ _ _])}
        #(do
           (receiver-handler session (mult event-chan) out-chan bytearray-fx)
           (>!! event-chan {:session session :event-type :rx-object-aborted})
           (is (nil? (poll! out-chan)))
           ))))
  (testing "handle closing stream (complete)"
    (println "At" *testing-contexts*)
    (let [session 1
          event-chan (chan 1)
          out-chan (timeout 100)]
      (with-redefs-fn {#'receive-data   (fn [_ _ _])}
        #(do
           (receiver-handler session (mult event-chan) out-chan bytearray-fx)
           (>!! event-chan {:session session :event-type :rx-object-completed})
           (is (nil? (poll! out-chan)))
           ))))
  (testing "handle unknown event type"
    (println "At" *testing-contexts*)
    (let [session 1
          event-chan (chan 1)
          out-chan (timeout 100)]
      (with-redefs-fn {#'close-receiver (fn [_ _])
                       #'receive-data   (fn [_ _ _]
                                        (>!! out-chan :received))}
        #(do
           (receiver-handler session (mult event-chan) out-chan bytearray-fx)
           (>!! event-chan {:session session :event-type :unknown})
           (is (= nil (poll! out-chan)))
           (>!! event-chan {:session session :event-type :rx-object-completed})
           (<!! out-chan)
           ))))
  (testing "handle different session for close"
    (println "At" *testing-contexts*)
    (let [session 1
          event-chan (chan 1)
          out-chan (timeout 100)
          ctl-chan (chan 1)]
      (with-redefs-fn {#'receive-data   (fn [_ _])}
        #(do
           (receiver-handler session (mult event-chan) out-chan nil-fx)
           (>!! event-chan {:session (inc session) :event-type :rx-object-completed})
           (is (nil? (poll! out-chan)))
           ))))
  (testing "receive some data"
    (println "At" *testing-contexts*)
    (let [session "sess"
          stream :stream
          event-chan (chan 5)
          out-chan (chan 5)
          ctl-chan (timeout 1000)]
      (with-redefs-fn {#'close-receiver (fn [_ _]),
                       #'receive-data   (fn [_ _ c]
                                          (>!! c :data)),
                       #'norm/seek-message-start (fn [_]
                                                   (>!! ctl-chan :ready)
                                                   true)}
        #(do
           (receiver-handler session (mult event-chan) out-chan nil-fx)
           (>!! event-chan {:session session,
                            :object stream,
                            :event-type :rx-object-new})
           ; todo race condition here
           (is (not (nil? (<!! ctl-chan)))) ; block until stream-handler is started
           (>!! event-chan {:session session,
                            :object stream,
                            :event-type :rx-object-updated})
           (>!! event-chan {:session session,
                            :object stream,
                            :event-type :rx-object-completed})
           (close! event-chan)
           (is (= :data (<!! out-chan)))
           ))))
  )
