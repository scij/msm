(ns com.senacor.msm.norm.norm-demo
  (:gen-class)
  (:require [com.senacor.msm.norm.control :as ctl]
            [com.senacor.msm.norm.receiver :as rcv]
            [com.senacor.msm.norm.sender :as snd]
            [com.senacor.msm.norm.msg :as msg]
            [clojure.core.async :refer [chan >!! <!! close! go-loop >! <!]]))

(defn sender
  [session args]
  (let [bytes-chan (chan 5)
        out-chan (chan 5)
        sndr (snd/create-sender session 0 bytes-chan 128)]
    (go-loop [i 1]
      (>! out-chan (msg/Message->bytes (msg/create-message "DEMO.COUNT" (str i))))
      (when (< i 10000)
        (recur (inc i))
        )
      )
    ))

(defn receiver
  [session args]
  (let [bytes-chan (chan 5)
        msg-chan (chan 5)
        rcvr (rcv/create-receiver session bytes-chan)
        msg-conv (msg/bytes->Messages bytes-chan msg-chan)]
    (go-loop [m (<! msg-chan)]
      (when m
        (println (:payload m))
        (recur (<! msg-chan))))
    ))

(defn -main [& args]
  (println (System/getProperty "java.library.path"))
  (ctl/init-norm)
  (let [session (ctl/start-norm-session "239.192.0.1" 7100 1)]
    (case (first args)
      "send" (sender session (rest args))
      "recv" (receiver session (rest args)))))
