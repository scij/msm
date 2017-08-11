(ns com.senacor.msm.core.receiver-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [chan mult timeout <!! >!!]]
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

(deftest test-command-handler
  (testing "one single command"
    (let [session 1
          event-chan (chan 1)
          cmd-chan (timeout 100)]
      (with-redefs-fn {#'norm/get-command (fn [_] (.getBytes "hallo"))
                       #'norm/get-local-node-id (fn [_] 1)}
        #(do
           (command-handler session (mult event-chan) cmd-chan)
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
           (command-handler session (mult event-chan) cmd-chan)
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

