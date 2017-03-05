(ns com.senacor.msm.norm.norm-simple-test
  (:require [com.senacor.msm.norm.norm-api :as norm])
  (:import (java.io IOException)))

(def repair-window-sz (* 1024 1024))
(def session-buffer-sz (* 1024 1024))
(def segment-sz 1400)
(def block-sz 64)
(def parity-segments 16)
(def dest-address "224.1.2.3")
(def dest-port 6003)
(def interface "en0")

(defn fill-buffer
  [index buffer len]
  (let [data (.getBytes (format "%08d" index))]
    (System/arraycopy data 0 buffer 0 8)))

(defn wait-for
  [event-set instance]
  (println "waiting for" event-set)
  (loop [event (norm/next-event instance)]
    (case (:event-type event)
      :tx-rate-changed (println "TX Rate changed" (norm/get-tx-rate (:session event)))
      :tx-grtt-updated (println "TX GRTT updated" (norm/get-grtt-estimate (:session event)))
      (println "Event" (:event-type event)))
    (when-not (contains? event-set (:event-type event))
      (recur (norm/next-event instance)))))

(defn send-to-stream
  [instance stream]
  (let [buf-len 1024
        buf (byte-array buf-len)]
    (doseq [i (range 1 10000)]
      (fill-buffer i buf buf-len)
      (println "loop" i)
      (loop [offset 0
             len buf-len
             num-written (norm/write-stream stream buf offset len)]
        (norm/mark-eom stream)
        (println "offset" offset "len" len "written" num-written)
        (when (not= num-written len)
          (wait-for #{:tx-queue-empty :tx-queue-vacancy} instance)
          (recur (+ offset num-written)
                 (- len num-written)
                 (norm/write-stream stream buf (+ offset num-written) (- len num-written)))))
      )))

(defn send-data []
  (let [instance (norm/create-instance)
        session (norm/create-session instance dest-address dest-port 1)]
    (norm/set-congestion-control session true true)
    ;(norm/set-loopback handle true)
    (norm/set-tx-rate session 80000000.0)
    (norm/set-grtt-estimate session 0.1)
    (norm/set-rx-port-reuse session true)
    (norm/set-multicast-interface session interface)
    (norm/set-loopback session true)
    (norm/start-sender session 1 session-buffer-sz segment-sz block-sz parity-segments)
    (let [stream (norm/open-stream session repair-window-sz)]
      (send-to-stream instance stream)
      (norm/close-stream stream true)
      )
    (wait-for #{:local-sender-closed} instance)
    (norm/stop-sender session)
    (norm/destroy-session session)
    (norm/destroy-instance instance)
    ))

(defn read-from-stream [stream]
  (let [buf-len 1024
        buf (byte-array buf-len)]
    (loop [num-read (norm/read-stream stream buf buf-len)]
      (when (< 0 num-read)
        (println "Block received")
        (recur (norm/read-stream stream buf buf-len))))))

(defn receive-data []
  (let [instance (norm/create-instance)
        session (norm/create-session instance dest-address dest-port 2)]
    ;(norm/set-default-unicast-nack session false)
    (norm/set-rx-port-reuse session true)
    (norm/set-loopback session true)
    (norm/set-multicast-interface session interface)
    (norm/start-receiver session session-buffer-sz)
    (println "Start receiver")
    (loop [event (norm/next-event instance)]
      (println "event received:" (:event-type event))
      (case (:event-type event)
        :rx-object-new
        (do
          (println "New stream")
          (recur (norm/next-event instance)))
        :rx-object-updated
        (do
          (read-from-stream (:object event))
          (recur (norm/next-event instance)))
        :rx-object-completed (println "End of stream")
        (do
          (println (:event-type event))
          (recur (norm/next-event instance)))))
    (norm/stop-receiver session)
    (norm/destroy-session session)
    (norm/destroy-instance instance)))

(defn -main [& args]
  (case (first args)
    "send" (send-data)
    "recv" (receive-data)
    :default (println "Nur send und recv sind erlaubt")))