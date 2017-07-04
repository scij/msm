(ns com.senacor.msm.norm.norm-demo
  (:gen-class)
  (:require [com.senacor.msm.norm.control :as ctl]
            [com.senacor.msm.norm.receiver :as rcv]
            [com.senacor.msm.norm.sender :as snd]
            [com.senacor.msm.norm.msg :as msg]
            [clojure.core.async :refer [chan >!! <!! close! mult tap]]
            [clojure.tools.logging :as log]
            [com.senacor.msm.norm.util :as util]
            [com.senacor.msm.norm.norm-api :as norm]))

(defn sender
  [session event-chan args]
  (let [out-chan (chan)
        sndr (snd/create-sender session 0 event-chan out-chan 128)]
    (doseq [i (range 10)]
      (log/tracef "Sending msg %d" i)
      (>!! out-chan (msg/Message->bytes (msg/create-message "DEMO.COUNT" (str "MSG="(inc i))))))
    (close! out-chan)
    ))


(defn receiver
  [session event-chan args]
  (let [bytes-chan (chan 5)
        msg-chan (chan 5)
        rcvr (rcv/create-receiver session event-chan bytes-chan)
        msg-conv (msg/bytes->Messages bytes-chan msg-chan)]
    (loop [m (<!! msg-chan)]
      (when m
        (log/tracef "Received message. Label=%s Payload=%s" (:label m) (:payload m))
        (recur (<!! msg-chan))))
    ))

(defn -main [& args]
  (println (System/getProperty "java.library.path"))
  (let [event-chan (chan 5)
        m-event-chan (mult event-chan)
        instance (ctl/init-norm event-chan)]
    (let [session (ctl/start-norm-session instance "239.192.0.1" 7100 1 :loopback true)]
      (ctl/mon-event-loop (tap m-event-chan (chan 5)))
      (case (first args)
        "send" (sender session (tap m-event-chan (chan 5)) (rest args))
        "recv" (receiver session (tap m-event-chan (chan 5)) (rest args)))
      (ctl/finit-norm instance))))
