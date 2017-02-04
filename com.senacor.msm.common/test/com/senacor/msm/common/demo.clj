(ns com.senacor.msm.common.demo
  (:gen-class)
  (:require [com.senacor.msm.common.net :as net]
            [com.senacor.msm.common.msg :as msg]
            [clojure.tools.logging :as log]))

;;
;; Kleines Testprogramm, um mit ZMQ herumzuspielen
;;


(defn server [net-spec label]
  (let [sock (net/create-server-socket net-spec label)]
    (log/trace "Server enter loop")
    (loop [req (net/zreceive sock)]
      (log/trace "Server received" req)
      (when-not (= (msg/get-payload req) "END")
        (recur (net/zreceive sock))))
    (net/close sock)
    (log/trace "Server exit")
    )
  )

(defn client [net-spec label]
  (let [sock (net/create-client-socket net-spec label)]
    (log/trace "Client enter loop")
    (doseq [i (range 100)]
      (log/trace "Client send" i)
      (net/zsend sock (msg/create-message label (str "Hello " i)))
      (Thread/sleep 500)
      )
    (net/zsend sock label "END")
    (log/trace "Client closing socket")
    (net/close sock)
    )
  )

(defn trace
  ([net-spec label]
   (println net-spec label)
   (let [sock (net/create-server-socket net-spec label)]
     (loop [msg (net/zreceive sock)]
       (println msg)
       (recur (net/zreceive sock)))))
  ([net-spec]
   (trace net-spec nil)))

(defn -main[& args]
  (case (first args)
    "server" (apply server (rest args))
    "client" (apply client (rest args))
    "trace"  (apply trace  (rest args))
    :else (println "Unknown command" (first args))))
