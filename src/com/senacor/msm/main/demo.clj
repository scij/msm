(ns com.senacor.msm.main.demo
  (:gen-class)
  (:require [com.senacor.msm.core.control :as ctl]
            [com.senacor.msm.core.receiver :as rcv]
            [com.senacor.msm.core.sender :as snd]
            [com.senacor.msm.core.message :as msg]
            [com.senacor.msm.core.util :as util]
            [com.senacor.msm.core.norm-api :as norm]
            [com.senacor.msm.core.monitor :as mon]
            [clojure.core.async :refer [chan >!! <!! close! mult tap]]
            [clojure.tools.logging :as log])
  (:import (org.apache.logging.log4j ThreadContext)))

(defn sender
  [session event-chan args]
  (let [out-chan (chan)
        cmd-chan (chan)
        sndr (snd/create-sender session 0 event-chan out-chan 128)
        count (if (and (not (empty args)) (re-matches #"\d+" (first args))) (Integer/parseInt (first args)) 10)]
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
        ; todo msg-conv (msg/bytes->Messages bytes-chan msg-chan)]
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
    (util/init-logging "demo")
    (let [session (ctl/start-session instance "" "239.192.0.1" 7100
                                     {:loopback true, :node-id (util/default-node-id) })]
      (mon/mon-event-loop event-chan-in)
      (case (first args)
        "send" (sender session event-chan-in  (rest args))
        "recv" (receiver session event-chan-in (rest args)))
      (ctl/finit-norm instance))))
