(ns com.senacor.msm.common.net-test
  (:require [clojure.test :refer :all]
            [com.senacor.msm.common.net :refer :all]
            [com.senacor.msm.common.msg :as msg])
  (:import (org.zeromq ZMQ$Socket)
           (com.senacor.msm.common.msg Message)))

(def label "test")
(deftest test-create-socket
  (testing "Create server"
    (with-open [s (create-server-socket "tcp://localhost:7777" label)]
      (is (instance? ZMQ$Socket s))
      ))
  (testing "Create client"
    (with-open [s (create-client-socket "tcp://localhost:7778")]
      (is (instance? ZMQ$Socket s))
      ))
  )

(deftest test-send-receive
  (testing "Ohne Filterung"
    (with-open [s (create-server-socket "inproc://test-send-rec" label)
                c (create-client-socket "inproc://test-send-rec")]
      (.start (Thread. ^Runnable (fn []
                                  (Thread/sleep 100)
                                  (println "DEB1 sending")
                                  (zsend c label "Payload")
                                  (println "DEB2 sent"))))
      (let [msg (zreceive s nil)]
        (is (instance? Message msg))
        (is (= (msg/get-label msg) label))
        (is (= (msg/get-payload msg) "Payload"))
        ))
    )
  (testing "Filter aktiv, String und match"
    (with-open [s (create-server-socket "inproc://test-send-rec" label)
                c (create-client-socket "inproc://test-send-rec")]
      (.start (Thread. ^Runnable (fn []
                                   (Thread/sleep 100)
                                   (println "DEB1 sending")
                                   (zsend c label "Payload")
                                   (println "DEB2 sent"))))
      (let [msg (zreceive s label)]
        (is (instance? Message msg))
        (is (= (msg/get-label msg) label))
        (is (= (msg/get-payload msg) "Payload"))
        ))
    )
  (testing "Filter aktiv, RE und beim ersten kein match"
    (with-open [s (create-server-socket "inproc://test-send-rec" #"te.+")
                c (create-client-socket "inproc://test-send-rec")]
      (.start (Thread. ^Runnable (fn []
                                   (Thread/sleep 100)
                                   (println "DEB1 sending")
                                   (zsend c "toast" "Payload")
                                   (println "DEB2 sending")
                                   (zsend c label "Payload"))))
      (let [msg (zreceive s #"te.+")]
        (is (instance? Message msg))
        (is (= (msg/get-label msg) label))
        (is (= (msg/get-payload msg) "Payload"))
        ))
    )
  )

