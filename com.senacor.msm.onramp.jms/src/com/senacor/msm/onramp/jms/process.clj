(ns com.senacor.msm.onramp.jms.process
  (:require [clojure.core.async :refer (buffer mult tap)]
            [com.senacor.msm.onramp.jms.jms :refer (jms->chan)]
            [com.senacor.msm.onramp.jms.store :refer (store)]
            [com.senacor.msm.common.async :refer (chan->zmq)]))

(defn process
  [jms-descr zmq-descr]
  (let [m-buf (mult (jms->chan jms-descr (buffer 10)))]
    (store (tap m-buf (buffer 10)))
    (chan->zmq s "RCSD.TEST.1" (tap m-buf (buffer 10))))
