(ns com.senacor.msm.core.control-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [chan <!!]]
            [com.senacor.msm.core.control :refer :all]
            [com.senacor.msm.core.norm-api :as norm]
            [clojure.tools.logging :as log]
            [com.senacor.msm.core.monitor :as monitor]))

(deftest test-event-loop
  (let [count (atom 0)]
    (with-redefs-fn {#'norm/next-event (fn [instance]
                                         (swap! count inc)
                                         (case @count
                                           1 {:session 1, :event-type :rx-object-new}
                                           2 {:session 1, :event-type :event-invalid}
                                           {:session 1, :event-type :zzz})
                                         )}
      #(let [event-chan (chan 1)
             evt (future (event-loop 1 event-chan))]
         (is (= {:session 1, :event-type :rx-object-new} (<!! event-chan)))
         (is (= {:session 1, :event-type :event-invalid} (<!! event-chan)))
         (is (nil? (<!! event-chan)))
         (is (= event-chan @evt))
         ))))

(deftest test-start-session
  (let [options (atom {})]
  (with-redefs-fn {#'norm/create-session          (fn [_ _ _ _]),
                   #'norm/set-multicast-interface (fn [_ if-name]
                                                    (swap! options assoc :if-name if-name)),
                   #'norm/set-ttl                 (fn [_ ttl]
                                                    (swap! options assoc :ttl ttl)),
                   #'norm/set-tos                 (fn [_ tos]
                                                    (swap! options assoc :tos tos)),
                   #'norm/set-loopback            (fn [_ v]
                                                    (swap! options assoc :loopback v)),
                   #'norm/set-rx-port-reuse       (fn [_ v]
                                                    (swap! options assoc :port-reuse v))
                   #'monitor/register             (fn [_ _ _ _])}
    #(do
       (start-session 1 "en0" "239.192.0.1" 7100
                      {:ttl 10,
                       :tos 40,
                      :loopback true})
       (is (= "en0" (:if-name @options)))
       (is (= 10 (:ttl @options)))
       (is (= 40 (:tos @options)))
       (is (:loopback @options))
       (is (:port-reuse @options))
       ))))
