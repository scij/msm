(ns com.senacor.msm.core.receiver-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [chan close! mult poll! mix admix tap timeout <!! >!!]]
            [com.senacor.msm.core.receiver :refer :all]
            [com.senacor.msm.core.norm-api :as norm]
            [com.senacor.msm.core.monitor :as monitor]
            [com.senacor.msm.core.monitor :as mon]
            [clojure.tools.logging :as log]))

(def bytearray-fx
  (map (fn [^bytes b] (String. b))))

(def fx-x
  (map (fn [x] x)))

(deftest test-receive-data
  (testing "one single message"
    (println "*** At" *testing-contexts*)
    (let [out-chan (chan 3)
          msgs (atom ["hallo" ""])]
      (with-redefs-fn {#'norm/read-stream (fn [stream buffer size]
                                            (let [this-msg (first @msgs)]
                                              (swap! msgs rest)
                                              (System/arraycopy (.getBytes this-msg) 0 buffer 0 (count this-msg))
                                              (count this-msg)))
                       #'monitor/record-bytes-received (fn [_ _])}
        #(do
           (receive-data 1 1 out-chan 1024)
           (is (= "hallo" (String. ^bytes (<!! out-chan))))
           )))
    )
  (testing "available data is bigger than buffer size"
    (println "*** At" *testing-contexts*)
    (let [out-chan (chan 3)
          msgs (atom ["hallo" "hallo" "end bag" ""])]
      (with-redefs-fn {#'norm/read-stream (fn [stream buffer size]
                                            (let [this-msg (first @msgs)]
                                              (swap! msgs rest)
                                              (System/arraycopy (.getBytes this-msg) 0 buffer 0 (count this-msg))
                                              (count this-msg)))
                       #'monitor/record-bytes-received (fn [_ _])}
        #(do
           (receive-data 1 1 out-chan 1024)
           (is (= "hallo" (String. ^bytes (<!! out-chan))))
           (is (= "hallo" (String. ^bytes (<!! out-chan))))
           (is (= "end bag" (String. ^bytes (<!! out-chan))))
           (is (nil? (poll! out-chan)))
           )
        )))
  )

(deftest test-stream-handler
  (testing "one message one matching event"
    (println "*** At" *testing-contexts*)
    (let [session 1
          stream :stream
          event-chan (chan 2)
          out-chan (chan 2)
          out-mix (mix out-chan)
          count (atom 0)]
      (with-redefs-fn {#'receive-data (fn [_ _ c _]
                                        (swap! count inc)
                                        (if (= 1 @count)
                                          (>!! c "hallo")
                                          (>!! c ""))),
                       #'norm/seek-message-start (fn [_] true),
                       #'norm/get-size (fn [_] 1024)}
        #(do
           (stream-handler session stream (mult event-chan) out-mix fx-x)
           (>!! event-chan {:session session :object stream :event-type :rx-object-updated})
           (>!! event-chan {:session session :object stream :event-type :rx-object-completed})
           (close! event-chan)
           (is (= "hallo" (<!! out-chan)))
           (is (= "" (<!! out-chan)))))))
  (testing "one message, event session NOT matching"
    (println "*** At" *testing-contexts*)
    (let [session 1
          stream :stream
          event-chan (chan 2)
          out-chan (chan 2)
          out-mix (mix out-chan)]
      (with-redefs-fn {#'receive-data (fn [_ _ c _] (>!! c "hallo")),
                       #'norm/seek-message-start (fn [_] true)
                       #'norm/get-size (fn [_] 1024)}
        #(do
            (stream-handler session stream (mult event-chan) out-mix fx-x)
            (>!! event-chan {:session 0, :object stream, :event-type :rx-object-updated})
            ; stream-handler fügt vorne ein event ein, weil wir vielleicht schon eins verpasst haben.
            (is (= "hallo" (<!! out-chan)))
            (is (nil? (poll! out-chan)))
            (>!! event-chan {:session session, :object stream, :event-type :rx-object-completed})
            (close! event-chan)))))
  )

(deftest test-receiver-handler
  (testing "handle immediate close without an event"
    (println "*** At" *testing-contexts*)
    (let [session 1
          event-chan (chan 1)
          out-chan (chan)]
      (with-redefs-fn {#'close-receiver (fn [session out-chan]
                                        (>!! out-chan :stopped))
                       #'receive-data   (fn [_ _ _ _]),
                       #'norm/get-size  (fn [_] 1024)}
        #(do
           (receiver-handler session (mult event-chan) out-chan bytearray-fx)
           (close! event-chan)
           (is (nil? (poll! out-chan)))
           ))))
  (testing "handle closing instance"
    (println "*** At" *testing-contexts*)
    (let [session 1
          event-chan (chan 1)
          out-chan (chan)]
      (with-redefs-fn {#'close-receiver (fn [session out-chan]
                                        (>!! out-chan :stopped))
                       #'receive-data   (fn [_ _ _ _])}
        #(do
           (receiver-handler session (mult event-chan) out-chan bytearray-fx)
           (>!! event-chan {:session session :event-type :event-invalid})
           (is (nil? (<!! out-chan)))
           ))))
  (testing "handle closing stream (aborted)"
    (println "*** At" *testing-contexts*)
    (let [session 1
          event-chan (chan 1)
          out-chan (timeout 100)]
      (with-redefs-fn {#'receive-data   (fn [_ _ _ _])}
        #(do
           (receiver-handler session (mult event-chan) out-chan bytearray-fx)
           (>!! event-chan {:session session :event-type :rx-object-aborted})
           (is (nil? (poll! out-chan)))
           ))))
  (testing "handle closing stream (complete)"
    (println "*** At" *testing-contexts*)
    (let [session 1
          event-chan (chan 1)
          out-chan (timeout 100)]
      (with-redefs-fn {#'receive-data   (fn [_ _ _ _])}
        #(do
           (receiver-handler session (mult event-chan) out-chan bytearray-fx)
           (>!! event-chan {:session session :event-type :rx-object-completed})
           (is (nil? (poll! out-chan)))
           ))))
  (testing "handle unknown event type"
    (println "*** At" *testing-contexts*)
    (let [session 1
          event-chan (chan 1)
          out-chan (timeout 100)]
      (with-redefs-fn {#'close-receiver (fn [_ _])
                       #'receive-data   (fn [_ _ _ _]
                                        (>!! out-chan :received))}
        #(do
           (receiver-handler session (mult event-chan) out-chan bytearray-fx)
           (>!! event-chan {:session session :event-type :unknown})
           (is (= nil (poll! out-chan)))
           (>!! event-chan {:session session :event-type :rx-object-completed})
           (<!! out-chan)
           ))))
  (testing "handle different session for close"
    (println "*** At" *testing-contexts*)
    (let [session 1
          event-chan (chan 1)
          out-chan (timeout 100)
          ctl-chan (chan 1)]
      (with-redefs-fn {#'receive-data   (fn [_ _ _ _])}
        #(do
           (receiver-handler session (mult event-chan) out-chan fx-x)
           (>!! event-chan {:session (inc session) :event-type :rx-object-completed})
           (is (nil? (poll! out-chan)))
           ))))
  (testing "receive some data"
    (println "*** At" *testing-contexts*)
    (let [session "sess"
          stream :stream
          event-chan (chan 5)
          out-chan (chan 5)
          ctl-chan (timeout 1000)]
      (with-redefs-fn {#'close-receiver (fn [_ _]),
                       #'receive-data   (fn [_ _ c _]
                                          (>!! c :data)),
                       #'norm/seek-message-start (fn [_]
                                                   (>!! ctl-chan :ready)
                                                   true)
                       #'norm/get-size (fn [_] 1024)}
        #(do
           (receiver-handler session (mult event-chan) out-chan fx-x)
           (>!! event-chan {:session session,
                            :object stream,
                            :event-type :rx-object-new})
           ; todo race condition here
           (is (not (nil? (<!! ctl-chan)))) ; block until stream-handler is started
           (>!! event-chan {:session session,
                            :object stream,
                            :event-type :rx-object-updated})
           (is (= :data (<!! out-chan)))
           (>!! event-chan {:session session,
                            :object stream,
                            :event-type :rx-object-completed})
           (close! event-chan)
           ))))
  )
