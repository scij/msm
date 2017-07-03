(ns com.senacor.msm.norm.norm-demo
  (:gen-class)
  (:require [com.senacor.msm.norm.control :as ctl]
            [com.senacor.msm.norm.receiver :as rcv]
            [com.senacor.msm.norm.sender :as snd]
            [com.senacor.msm.norm.msg :as msg]
            [clojure.core.async :refer [chan >!! <!! close! go-loop >! <!]]
            [clojure.tools.logging :as log]
            [com.senacor.msm.norm.util :as util]
            [com.senacor.msm.norm.norm-api :as norm]))

(defn sender
  [instance session args]
  (let [out-chan (chan 5)
        sndr (snd/create-sender session 0 out-chan 128)]
    (go-loop [i 1]
      (log/tracef "Sending msg %d" i)
      (>! out-chan (msg/Message->bytes (msg/create-message "DEMO.COUNT" (str i))))
      (if (< i 10)
        (recur (inc i))
        (close! out-chan)))
    ))


(defn receiver
  [session args]
  (let [bytes-chan (chan 5)
        msg-chan (chan 5)
        rcvr (rcv/create-receiver session bytes-chan)
        msg-conv (msg/bytes->Messages bytes-chan msg-chan)]
    (go-loop [m (<! msg-chan)]
      (when m
        (log/tracef "Received message. Label=%s Payload=%s" (:label m) (:payload m))
        (recur (<! msg-chan))))
    ))

(defn -main [& args]
  (println (System/getProperty "java.library.path"))
  (let [instance (ctl/init-norm)]
    (let [session (ctl/start-norm-session instance "239.192.0.1" 7100 1 :loopback true)]
      (case (first args)
        "send" (sender instance session (rest args))
        "recv" (receiver session (rest args))))))
