(ns com.senacor.msm.common.demo
  (:gen-class)
  (:require [com.senacor.msm.common.net :as net]
            [com.senacor.msm.common.msg :as msg]))

;;
;; Kleines Testprogramm, um mit ZMQ herumzuspielen
;;

(defn kurzschluss []
  (with-open [requester (net/create-client-socket "tcp://239.0.0.1:5559")]
    (dotimes [i 10]
      (println "About to send" i)
      (net/zsend requester "TEST" "Hello")
      (println "Sent" i "and receiving")
      (let [msg (net/zreceive requester nil)]
        (printf "Received reply %d [%s]\n" i msg)))))


(defn server [net-spec label]
  (let [sock (net/create-server-socket net-spec label)]
    (println "Server enter loop")
    (loop [req (net/zreceive sock label)]
      (println "Server received" req)
      (when-not (= (msg/get-payload req) "END")
        (recur (net/zreceive sock label))))
    (net/close sock)
    (println "Server exit")
    )
  )

(defn client [net-spec label]
  (let [sock (net/create-client-socket net-spec)]
    (println "Client enter loop")
    (doseq [i (range 10)]
      (println "Client send" i)
      (net/zsend sock (msg/create-message label (str "Hello " i)))
      )
    (net/zsend sock "END")
    (println "Client closing socket")
    (net/close sock)
    )
  )

(defn -main[& args]
  (if (= "server" (first args))
    (apply server (rest args))
    (apply client (rest args)))
  )