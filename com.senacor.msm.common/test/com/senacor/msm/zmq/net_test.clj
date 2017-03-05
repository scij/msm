(ns com.senacor.msm.zmq.net-test
  (:require [clojure.test :refer :all]
            [com.senacor.msm.zmq.net :refer :all]
            [com.senacor.msm.zmq.msg :as msg])
  (:import (org.zeromq ZMQ$Socket)
           (com.senacor.msm.zmq.msg Message)))

(def label "test create sockets")

(deftest test-create-socket
  (testing "Create server tcp"
    (with-open [s (create-server-socket "tcp://127.0.0.1:7777" label)]
      (is s)
      (is (instance? ZMQ$Socket (:socket s)))
      ))
  (testing "Create client tcp"
    (with-open [s (create-client-socket "tcp://localhost:7778" label)]
      (is s)
      (is (instance? ZMQ$Socket (:socket s)))
      ))
  (testing "Create server epgm"
    (with-open [s (create-server-socket "epgm://en0;239.0.0.1:7777" label)]
      (is s)
      (is (instance? ZMQ$Socket (:socket s)))
      ))
  (testing "Create client epgm"
    (with-open [s (create-client-socket "epgm://en0;239.0.0.1:7777" label)]
      (is s)
      (is (instance? ZMQ$Socket (:socket s)))
      ))
  ;(testing "Create server pgm"
  ;  (with-open [s (create-server-socket "pgm://en0;239.0.0.1:7777" label)]
  ;    (is (instance? ZMQ$Socket s))
  ;    ))
  ;(testing "Create client pgm"
  ;  (with-open [s (create-client-socket "pgm://en0;239.0.0.1:7777")]
  ;    (is (instance? ZMQ$Socket s))
  ;    ))
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
      (let [msg (zreceive s)]
        (is (instance? Message msg))
        (is (= (msg/get-label msg) label))
        (is (= (msg/get-payload msg) "Payload"))
        ))
    )
  (testing "Filter aktiv, String und match"
    (with-open [s (create-server-socket "inproc://test-send-rec" label)
                c (create-client-socket "inproc://test-send-rec" label)]
      (.start (Thread. ^Runnable (fn []
                                   (Thread/sleep 100)
                                   (println "DEB1 sending")
                                   (zsend c label "Payload")
                                   (println "DEB2 sent"))))
      (let [msg (zreceive s)]
        (is (instance? Message msg))
        (is (= (msg/get-label msg) label))
        (is (= (msg/get-payload msg) "Payload"))
        ))
    )
  (testing "Filter aktiv, RE und beim ersten kein match"
    (with-open [s (create-server-socket "inproc://test-send-rec" #"te.+")
                c (create-client-socket "inproc://test-send-rec" #"te.+")]
      (.start (Thread. ^Runnable (fn []
                                   (Thread/sleep 100)
                                   (println "DEB1 sending")
                                   (zsend c "toast" "Payload")
                                   (println "DEB2 sending")
                                   (zsend c label "Payload"))))
      (let [msg (zreceive s)]
        (is (instance? Message msg))
        (is (= (msg/get-label msg) label))
        (is (= (msg/get-payload msg) "Payload"))
        ))
    )
  )

