(ns com.senacor.msm.onramp.jms.process
  (:require [clojure.core.async :refer (buffer)]
            [com.senacor.msm.onramp.jms.jms :refer (jms->chan)]
            [com.senacor.msm.common.async :refer (chan->zmq)]))

(defn process
  []
  (let [buf (buffer 10)]
    (jms->chan buf)
    (chan->zmq s "RCSD.TEST.1" buf)))
