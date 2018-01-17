(ns com.senacor.msm.core.listen-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [chan >!! close!]]
            [com.senacor.msm.main.listen :refer :all]
            [clojure.java.io :as io]))

(deftest test-print-to-file
  (testing "A single line"
    (let [c (chan 1)
          fn "target/test.out"]
      (.delete (io/file fn))
      (print-to-file fn c)
      (>!! c "line 1")
      (close! c)
      (Thread/sleep 100)
      (is (.exists (io/file fn)))
      (is (= 9 (.length (io/file fn))))))
  (testing "Many lines"
    (let [c (chan 3)
          fn "target/test.out"]
      (.delete (io/file fn))
      (print-to-file fn c)
      (>!! c "line 1")
      (>!! c "line 2")
      (>!! c "line 3")
      (close! c)
      (Thread/sleep 100)
      (is (.exists (io/file fn)))
      (is (= 27 (.length (io/file fn))))))
  (testing "empty"
    (let [c (chan 1)
          fn "target/test.out"]
      (.delete (io/file fn))
      (print-to-file fn c)
      (close! c)
      (Thread/sleep 100)
      (is (.exists (io/file fn)))))
  )
