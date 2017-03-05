(ns com.senacor.msm.zmq.demo
  (:gen-class)
  (:require [com.senacor.msm.zmq.net :as net]
            [com.senacor.msm.zmq.msg :as msg]
            [clojure.tools.logging :as log]
            [zeromq.zmq :as zmq]
            [zeromq.device :as device]))

;;
;; Kleines Testprogramm, um mit ZMQ herumzuspielen
;;


(defn server [net-spec label]
  (let [sock (net/create-server-socket net-spec label)]
    (log/trace "Server enter loop")
    (loop [req (net/zreceive sock)
           num-msgs 1]
      (log/trace "Server received" req)
      (if (= (msg/get-payload req) "END")
        (log/tracef "End received after %d messages" num-msgs)
        (recur (net/zreceive sock) (inc num-msgs))))
    (net/close sock)
    (log/trace "Server exit")
    )
  )

(defn client [net-spec label]
  (let [sock (net/create-client-socket net-spec label)]
    (log/trace "Client enter loop")
    (doseq [i (range 100000)]
      (when (zero? (rem i 100))
        (log/trace "Client send" i))
      (net/zsend sock (msg/create-message label (str "Hello " i)))
      ;(Thread/sleep 5)
      )
    (net/zsend sock (msg/create-message label "END"))
    (log/trace "Client closing socket")
    (net/close sock)
    )
  )

(defn trace
  ([net-spec label]
   (log/trace net-spec label)
   (let [sock (net/create-server-socket net-spec label)]
     (loop [msg (net/zreceive sock)]
       (log/trace msg)
       (recur (net/zreceive sock)))))
  ([net-spec]
   (trace net-spec nil)))

(defn -main[& args]
  (case (first args)
    "server" (apply server (rest args))
    "client" (apply client (rest args))
    "trace"  (apply trace  (rest args))
    :else (println "Unknown command" (first args))))
