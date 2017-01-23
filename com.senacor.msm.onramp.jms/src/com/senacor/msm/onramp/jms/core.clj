(ns com.senacor.msm.onramp.core
  (:require [com.senacor.msm.onramp.jms.process :refer (process)])
  (:gen-class)
  (:import (javax.jms Session)))

(defn -main
  [& args]

  (process {:jms-host   "tcp://localhost:7778",
            :queue-name "RCSD.Q.1"
            :ack-mode   Session/AUTO_ACKNOWLEDGE}
           "RCSD.TEST.1")
)
