(ns com.senacor.msm.norm.norm-demo
  (:gen-class)
  (:require [com.senacor.msm.norm.control :as ctl]
            [com.senacor.msm.norm.receiver :as rcv]
            [com.senacor.msm.norm.sender :as snd]
            [com.senacor.msm.norm.msg :as msg]
            [clojure.core.async :refer [chan >!! <!! close! mult tap]]
            [clojure.tools.logging :as log]
            [com.senacor.msm.norm.util :as util]
            [com.senacor.msm.norm.norm-api :as norm]
            [com.senacor.msm.norm.mon :as mon]))

(defn sender
  [session event-chan args]
  (let [out-chan (chan)
        sndr (snd/create-sender session 0 event-chan out-chan 128)
        count (if (re-matches #"\d+" (first args)) (Integer/parseInt (first args)) 10)]
    (doseq [i (range 1 (inc count))]
      (log/tracef "Sending msg %d" i)
      (>!! out-chan (msg/Message->bytes (msg/create-message "DEMO.COUNT" (str "MSG="i)))))
    (close! out-chan)
    ))


(defn receiver
  [session event-chan args]
  (let [bytes-chan (chan 5)
        msg-chan (chan 5)
        rcvr (rcv/create-receiver session event-chan bytes-chan)
        msg-conv (msg/bytes->Messages bytes-chan msg-chan)]
    (loop [m (<!! msg-chan)
           exp-count 1]
      (when m
        (let [count (Integer/parseInt (second (re-matches #"MSG=(\d+)" (:payload m))))]
          (when (not= exp-count count)
            (log/warnf "Missing message: expected=%d payload=%d" exp-count count)))
        (log/tracef "Received message. Label=%s Payload=%s" (:label m) (:payload m))
        (recur (<!! msg-chan)
               (inc exp-count))))
    ))

(defn -main [& args]
  (let [event-chan-out (chan 5)
        event-chan-in (mult event-chan-out)
        instance (ctl/init-norm event-chan-out)]
    (let [session (ctl/start-norm-session instance "239.192.0.1" 7100 1 :loopback true)]
      (mon/mon-event-loop event-chan-in)
      (case (first args)
        "send" (sender session event-chan-in  (rest args))
        "recv" (receiver session event-chan-in (rest args)))
      (ctl/finit-norm instance))))
