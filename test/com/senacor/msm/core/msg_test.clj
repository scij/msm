(ns com.senacor.msm.core.msg-test
  (:require [com.senacor.msm.core.message :refer :all]
            [com.senacor.msm.core.util :as util]
            [clojure.test :refer :all]
            [bytebuffer.buff :as bb]
            [clojure.core.async :refer [<!! >!! <! >! chan to-chan timeout close!]])
  (:import (java.nio ByteBuffer)
           (java.util UUID)))

(def fix-msg (create-message "label" "uuid" 0 "payload"))

(def fix-buflen (message-length "label" "uuid" "payload"))

(defn bytes-eq?
  [^bytes b1 ^bytes b2]
   (if (= (count b1) (count b2))
     (every? identity (map = b1 b2))
     false))


(deftest test-encode
  (testing "just encode"
    (let [fxb ^bytes (Message->bytes fix-msg)]
      (is fxb)
      (is (= fix-buflen (count fxb)))))

  (testing "encode and inspect"
    (let [fxb (ByteBuffer/wrap (Message->bytes fix-msg))]
      (is (= fix-buflen (.remaining fxb)))
      (is (= 77 (bb/take-byte fxb)))
      (is (= 88 (bb/take-byte fxb)))
      (is (= 1  (bb/take-byte fxb)))
      (is (= 0  (bb/take-byte fxb)))
      (is (= 19 (bb/take-short fxb)))
      (is (= 7  (bb/take-int fxb)))
      (is (= 0  (bb/take-long fxb)))
      (is (= 5  (bb/take-byte fxb)))
      (is (= (byte \l) (bb/take-byte fxb)))
      (is (= (byte \a) (bb/take-byte fxb)))
      (is (= (byte \b) (bb/take-byte fxb)))
      (is (= (byte \e) (bb/take-byte fxb)))
      (is (= (byte \l) (bb/take-byte fxb)))
      (is (= 4 (bb/take-byte fxb)))
      (is (= (byte \u) (bb/take-byte fxb)))
      (is (= (byte \u) (bb/take-byte fxb)))
      (is (= (byte \i) (bb/take-byte fxb)))
      (is (= (byte \d) (bb/take-byte fxb)))
      (is (= (byte \p) (bb/take-byte fxb)))
      (is (= (byte \a) (bb/take-byte fxb)))
      (is (= (byte \y) (bb/take-byte fxb)))
      (is (= (byte \l) (bb/take-byte fxb)))
      (is (= (byte \o) (bb/take-byte fxb)))
      (is (= (byte \a) (bb/take-byte fxb)))
      (is (= (byte \d) (bb/take-byte fxb)))
      (is (= 0 (.remaining fxb)))))

  (testing "encode and parse"
    (let [cid (str (UUID/randomUUID))
          msg (create-message "/com/senacor/msm" cid 0 "MSG 123")
          fxb (ByteBuffer/wrap (Message->bytes msg))]
      (is (msg= msg (parse-message (.array fxb)))))))



(defn fill-buffer-with-testdata
  "Fill buf with predefined test data and flip the buffer.
  When no-flip is set, leave the buffer unchanged"
  [^ByteBuffer buf & no-flip]
  (doto buf
    (bb/put-byte 77)
    (bb/put-byte 88)
    (bb/put-byte 1)
    (bb/put-byte 0)
    (bb/put-short 19)
    (bb/put-int 7)
    (bb/put-long 0)
    (bb/put-byte 5)
    (bb/put-byte (byte \l))
    (bb/put-byte (byte \a))
    (bb/put-byte (byte \b))
    (bb/put-byte (byte \e))
    (bb/put-byte (byte \l))
    (bb/put-byte 4)
    (bb/put-byte (byte \u))
    (bb/put-byte (byte \u))
    (bb/put-byte (byte \i))
    (bb/put-byte (byte \d))
    (bb/put-byte (byte \p))
    (bb/put-byte (byte \a))
    (bb/put-byte (byte \y))
    (bb/put-byte (byte \l))
    (bb/put-byte (byte \o))
    (bb/put-byte (byte \a))
    (bb/put-byte (byte \d)))
  (when-not no-flip
    (println "Flipping buffer" buf)
    (.flip buf))
  buf)

(deftest test-buffer-io
  (testing "Puffer vergleichen"
    (let [buf (fill-buffer-with-testdata (bb/byte-buffer fix-buflen) :no-flip)]
      (is (util/byte-array-equal (.array buf) (Message->bytes fix-msg)))
      (is (= (String. ^"[B" (.array buf))
             (String. (Message->bytes fix-msg))))))

  (testing "buffer io mit mark und reset"
    (let [buf (bb/byte-buffer 20)]
      (.mark buf)
      (is (= 20 (.remaining buf)))
      (is (= 0 (.position buf)))
      (bb/put-byte buf 1)
      (bb/put-byte buf 2)
      (is (= 2 (.position buf)))
      (is (= 18 (.remaining buf)))
      (.reset buf)
      (is (= 20 (.remaining buf)))
      (is (= 0 (.position buf)))
      (is (= 1 (bb/take-byte buf)))
      (is (= 2 (bb/take-byte buf)))
      (.mark buf)
      (bb/put-byte buf 3)
      (bb/put-byte buf 4)
      (.reset buf)
      (.compact buf)
      (.flip buf)
      (is (= 0 (.position buf)))
      (is (= 18 (.remaining buf)))))

  (testing "buffer io with flip"
    (let [buf (bb/byte-buffer 20)]
      (is (= 20 (.remaining buf)))
      (bb/put-byte buf 1)
      (bb/put-byte buf 2)
      (.flip buf)
      (is (= 2 (.remaining buf)))))

  (testing "buffer out with flip"
    (let [buf (bb/byte-buffer 20)]
      (is (= 20 (.remaining buf)))
      (bb/put-byte buf (byte \a))
      (bb/put-byte buf (byte \b))
      (.flip buf)
      (bb/put-byte buf (byte \c))
      (bb/put-byte buf (byte \d))
      (is (= 0 (.remaining buf))) ;; flip has set the limit to 2
      (is (= "cd" (String. ^bytes (.array buf) 0 2))))))



(deftest test-async-io
  (testing "send and close"
    (let [c (chan 1)]
      (>!! c "hi")
      (close! c)
      (is (= "hi" (<!! c)))
      (is (nil? (<!! c))))))

(deftest test-align-buffer
  (testing "One array - one message"
    (let [fix (.array (fill-buffer-with-testdata (bb/byte-buffer fix-buflen) true))]
      (is (util/byte-array-equal (first (into [] (align-byte-arrays) [fix])) fix))))
  (testing "one array - two messages"
    (let [fix (byte-array (* 2 fix-buflen))
          one-fix (byte-array fix-buflen)
          buf (ByteBuffer/wrap fix)]
      (fill-buffer-with-testdata buf :no-flip)
      (fill-buffer-with-testdata buf)
      (fill-buffer-with-testdata (ByteBuffer/wrap one-fix))
      (is (= 2 (count (into [] (align-byte-arrays) [fix]))))
      (is (= fix-buflen (count (first (into [] (align-byte-arrays) [fix])))))
      (is (= fix-buflen (count (second (into [] (align-byte-arrays) [fix])))))
      (is (util/byte-array-equal (first (into [] (align-byte-arrays) [fix])) one-fix))
      (is (util/byte-array-equal (second (into [] (align-byte-arrays) [fix])) one-fix))))
  (testing "two arrays with each one message"
    (let [f1 (byte-array fix-buflen)
          f2 (byte-array fix-buflen)]
      (fill-buffer-with-testdata (ByteBuffer/wrap f1))
      (fill-buffer-with-testdata (ByteBuffer/wrap f2))
      (is (= 2 (count (into [] (align-byte-arrays) [f1 f2]))))
      (is (= fix-buflen (count (first (into [] (align-byte-arrays) [f1 f2])))))
      (is (= fix-buflen (count (second (into [] (align-byte-arrays) [f1 f2])))))
      (is (util/byte-array-equal (first (into [] (align-byte-arrays) [f1 f2])) f1))
      (is (util/byte-array-equal (second (into [] (align-byte-arrays) [f1 f2])) f2))))
  (testing "two arrays message split directly behind header"
    (let [fix (byte-array (* 2 fix-buflen))
          one-fix (byte-array fix-buflen)
          buf (ByteBuffer/wrap fix)]
      (fill-buffer-with-testdata buf :no-flip)
      (fill-buffer-with-testdata buf)
      (fill-buffer-with-testdata (ByteBuffer/wrap one-fix))
      (let [f1 (util/byte-array-head fix 10)
            f2 (util/byte-array-rest fix 10)]
        (is (= 2 (count (into [] (align-byte-arrays) [f1 f2]))))
        (is (= fix-buflen (count (first (into [] (align-byte-arrays) [f1 f2])))))
        (is (= fix-buflen (count (second (into [] (align-byte-arrays) [f1 f2])))))
        (is (util/byte-array-equal (first (into [] (align-byte-arrays) [f1 f2])) one-fix))
        (is (util/byte-array-equal (second (into [] (align-byte-arrays) [f1 f2])) one-fix)))))
  (testing "two arrays message split inside header"
    (let [fix (byte-array (* 2 fix-buflen))
          one-fix (byte-array fix-buflen)
          buf (ByteBuffer/wrap fix)]
      (fill-buffer-with-testdata buf :no-flip)
      (fill-buffer-with-testdata buf)
      (fill-buffer-with-testdata (ByteBuffer/wrap one-fix))
      (let [f1 (util/byte-array-head fix 5)
            f2 (util/byte-array-rest fix 5)]
        (is (= 2 (count (into [] (align-byte-arrays) [f1 f2]))))
        (is (= fix-buflen (count (first (into [] (align-byte-arrays) [f1 f2])))))
        (is (= fix-buflen (count (second (into [] (align-byte-arrays) [f1 f2])))))
        (is (util/byte-array-equal (first (into [] (align-byte-arrays) [f1 f2])) one-fix))
        (is (util/byte-array-equal (second (into [] (align-byte-arrays) [f1 f2])) one-fix)))))
  (testing "two arrays message split inside payload"
    (let [fix (byte-array (* 2 fix-buflen))
          one-fix (byte-array fix-buflen)
          buf (ByteBuffer/wrap fix)]
      (fill-buffer-with-testdata buf :no-flip)
      (fill-buffer-with-testdata buf)
      (fill-buffer-with-testdata (ByteBuffer/wrap one-fix))
      (let [f1 (util/byte-array-head fix 20)
            f2 (util/byte-array-rest fix 20)]
        (is (= 2 (count (into [] (align-byte-arrays) [f1 f2]))))
        (is (= fix-buflen (count (first (into [] (align-byte-arrays) [f1 f2])))))
        (is (= fix-buflen (count (second (into [] (align-byte-arrays) [f1 f2])))))
        (is (util/byte-array-equal (first (into [] (align-byte-arrays) [f1 f2])) one-fix))
        (is (util/byte-array-equal (second (into [] (align-byte-arrays) [f1 f2])) one-fix)))))
  (testing "three arrays message split inside payload"
    (let [fix (byte-array (* 2 fix-buflen))
          one-fix (byte-array fix-buflen)
          buf (ByteBuffer/wrap fix)]
      (fill-buffer-with-testdata buf :no-flip)
      (fill-buffer-with-testdata buf)
      (fill-buffer-with-testdata (ByteBuffer/wrap one-fix))
      (let [f1 (util/byte-array-head fix 15)
            f2 (util/byte-array-head (util/byte-array-rest fix 15) 25)
            f3 (util/byte-array-rest fix 40)]
        (is (= 2 (count (into [] (align-byte-arrays) [f1 f2 f3]))))
        (is (= fix-buflen (count (first (into [] (align-byte-arrays) [f1 f2 f3])))))
        (is (= fix-buflen (count (second (into [] (align-byte-arrays) [f1 f2 f3])))))
        (is (util/byte-array-equal (first (into [] (align-byte-arrays) [f1 f2 f3])) one-fix))
        (is (util/byte-array-equal (second (into [] (align-byte-arrays) [f1 f2 f3])) one-fix))))))


(deftest test-label-match
  (testing "nil match"
    (let [fix (create-message "foo" "bar")]
      (is (label-match nil fix))))
  (testing "string match"
    (let [fix (create-message "foo" "bar")]
      (is (label-match "foo" fix))
      (is (not (label-match "bar" fix)))
      (is (not (label-match "f" fix)))
      (is (not (label-match "fo" fix)))
      (is (not (label-match "fooo" fix)))))
  (testing "regex match"
    (let [fix (create-message "com.senacor.msm.label.1" "foo.bar")]
      (is (label-match #"com.+" fix))
      (is (label-match #"com.senacor.*" fix)))))


(deftest test-parse-message
  (testing "well formed message"
    (let [bb (bb/byte-buffer fix-buflen)]
      (fill-buffer-with-testdata bb)
      (is (msg= fix-msg (parse-message (.array bb))))))
  (testing "message with bad magic number first char"
    (let [bb (bb/byte-buffer fix-buflen)]
      (doto bb
        (bb/put-byte 76)
        (bb/put-byte 88)
        (bb/put-byte 1)
        (bb/put-byte 0)
        (bb/put-short 12)
        (bb/put-int 1)
        (bb/put-long 0)
        (bb/put-byte 1)
        (bb/put-byte (byte \l))
        (bb/put-byte 1)
        (bb/put-byte (byte \i))
        (bb/put-byte (byte \p))
        (bb/put-byte (byte \d)))
      (.flip bb)
      (is (= [0 0] (parse-fixed-header bb)))))
  (testing "message with bad magic number second char"
    (let [bb (bb/byte-buffer fix-buflen)]
      (doto bb
        (bb/put-byte 77)
        (bb/put-byte 87)
        (bb/put-byte 1)
        (bb/put-byte 0)
        (bb/put-short 12)
        (bb/put-int 1)
        (bb/put-long 0)
        (bb/put-byte 1)
        (bb/put-byte (byte \l))
        (bb/put-byte 1)
        (bb/put-byte (byte \i))
        (bb/put-byte (byte \p))
        (bb/put-byte (byte \d)))
      (.flip bb)
      (is (= [0 0] (parse-fixed-header bb)))))
  (testing "message with bad version number (major)"
    (let [bb (bb/byte-buffer fix-buflen)]
      (doto bb
        (bb/put-byte 77)
        (bb/put-byte 88)
        (bb/put-byte 2)
        (bb/put-byte 0)
        (bb/put-short 12)
        (bb/put-int 1)
        (bb/put-long 0)
        (bb/put-byte 1)
        (bb/put-byte (byte \l))
        (bb/put-byte 1)
        (bb/put-byte (byte \i))
        (bb/put-byte (byte \p))
        (bb/put-byte (byte \d)))
      (.flip bb)
      (is (= [0 0] (parse-fixed-header bb)))))
  (testing "message with bad version number (minor)"
    (let [bb (bb/byte-buffer fix-buflen)]
      (doto bb
        (bb/put-byte 77)
        (bb/put-byte 88)
        (bb/put-byte 1)
        (bb/put-byte 1)
        (bb/put-short 12)
        (bb/put-int 1)
        (bb/put-long 0)
        (bb/put-byte 1)
        (bb/put-byte (byte \l))
        (bb/put-byte 1)
        (bb/put-byte (byte \i))
        (bb/put-byte (byte \p))
        (bb/put-byte (byte \d)))
      (.flip bb)
      (is (= [0 0] (parse-fixed-header bb)))))
  (testing "message with bad version number (minor)"
    (let [bb (bb/byte-buffer fix-buflen)]
      (doto bb
        (bb/put-byte 77)
        (bb/put-byte 88)
        (bb/put-byte 1)
        (bb/put-byte 1)
        (bb/put-short 9)
        (bb/put-int 1)
        (bb/put-long 0)
        (bb/put-byte 1)
        (bb/put-byte (byte \l))
        (bb/put-byte 1)
        (bb/put-byte (byte \i))
        (bb/put-byte (byte \p))
        (bb/put-byte (byte \d)))
      (.flip bb)
      (is (= [0 0] (parse-fixed-header bb)))))
  (testing "message with bad var header length"
    (let [bb (bb/byte-buffer fix-buflen)]
      (doto bb
        (bb/put-byte 77)
        (bb/put-byte 88)
        (bb/put-byte 1)
        (bb/put-byte 0)
        (bb/put-short 3) ; here
        (bb/put-int 1)
        (bb/put-long 0)
        (bb/put-byte 1)
        (bb/put-byte (byte \l))
        (bb/put-byte 1)
        (bb/put-byte (byte \i))
        (bb/put-byte (byte \p))
        (bb/put-byte (byte \d)))
      (.flip bb)
      (is (= [0 0] (parse-fixed-header bb)))))
  (testing "message with bad var header length"
    (let [bb (bb/byte-buffer fix-buflen)]
      (doto bb
        (bb/put-byte 77)
        (bb/put-byte 88)
        (bb/put-byte 1)
        (bb/put-byte 0)
        (bb/put-short 4)
        (bb/put-int -1) ; here
        (bb/put-long 0)
        (bb/put-byte 1)
        (bb/put-byte (byte \l))
        (bb/put-byte 1)
        (bb/put-byte (byte \i))
        (bb/put-byte (byte \p))
        (bb/put-byte (byte \d)))
      (.flip bb)
      (is (= [0 0] (parse-fixed-header bb))))))


(deftest test-rebuild-message
  (testing "one message"
    (let [fix (.array (fill-buffer-with-testdata (bb/byte-buffer fix-buflen) true))]
      (is (msg= fix-msg (first (into [] message-rebuilder [fix]))))))
  (testing "one array - two messages"
    (let [fix (byte-array (* 2 fix-buflen))
          one-fix (byte-array fix-buflen)
          buf (ByteBuffer/wrap fix)]
      (fill-buffer-with-testdata buf :no-flip)
      (fill-buffer-with-testdata buf)
      (fill-buffer-with-testdata (ByteBuffer/wrap one-fix))
      (is (= 2 (count (into [] message-rebuilder [fix]))))
      (is (msg= fix-msg (first (into [] message-rebuilder [fix]))))
      (is (msg= fix-msg (second (into [] message-rebuilder [fix]))))))
  (testing "two arrays with each one message"
    (let [f1 (byte-array fix-buflen)
          f2 (byte-array fix-buflen)]
      (fill-buffer-with-testdata (ByteBuffer/wrap f1))
      (fill-buffer-with-testdata (ByteBuffer/wrap f2))
      (is (= 2 (count (into [] message-rebuilder [f1 f2]))))
      (is (msg= fix-msg (first (into [] message-rebuilder [f1 f2]))))
      (is (msg= fix-msg (second (into [] message-rebuilder [f1 f2]))))))
  (testing "two arrays message split directly behind header"
    (let [fix (byte-array (* 2 fix-buflen))
          one-fix (byte-array fix-buflen)
          buf (ByteBuffer/wrap fix)]
      (fill-buffer-with-testdata buf :no-flip)
      (fill-buffer-with-testdata buf)
      (fill-buffer-with-testdata (ByteBuffer/wrap one-fix))
      (let [f1 (util/byte-array-head fix 10)
            f2 (util/byte-array-rest fix 10)]
        (is (= 2 (count (into [] message-rebuilder [f1 f2]))))
        (is (msg= fix-msg (first (into [] message-rebuilder [f1 f2]))))
        (is (msg= fix-msg (second (into [] message-rebuilder [f1 f2])))))))
  (testing "two arrays message split inside header"
    (let [fix (byte-array (* 2 fix-buflen))
          one-fix (byte-array fix-buflen)
          buf (ByteBuffer/wrap fix)]
      (fill-buffer-with-testdata buf :no-flip)
      (fill-buffer-with-testdata buf)
      (fill-buffer-with-testdata (ByteBuffer/wrap one-fix))
      (let [f1 (util/byte-array-head fix 5)
            f2 (util/byte-array-rest fix 5)]
        (is (= 2 (count (into [] message-rebuilder [f1 f2]))))
        (is (msg= fix-msg (first (into [] message-rebuilder [f1 f2]))))
        (is (msg= fix-msg (second (into [] message-rebuilder [f1 f2])))))))
  (testing "two arrays message split inside payload"
    (let [fix (byte-array (* 2 fix-buflen))
          one-fix (byte-array fix-buflen)
          buf (ByteBuffer/wrap fix)]
      (fill-buffer-with-testdata buf :no-flip)
      (fill-buffer-with-testdata buf)
      (fill-buffer-with-testdata (ByteBuffer/wrap one-fix))
      (let [f1 (util/byte-array-head fix 20)
            f2 (util/byte-array-rest fix 20)]
        (is (= 2 (count (into [] message-rebuilder [f1 f2]))))
        (is (msg= fix-msg (first (into [] message-rebuilder [f1 f2]))))
        (is (msg= fix-msg (second (into [] message-rebuilder [f1 f2])))))))
  (testing "three arrays message split inside payload"
    (let [fix (byte-array (* 2 fix-buflen))
          one-fix (byte-array fix-buflen)
          buf (ByteBuffer/wrap fix)]
      (fill-buffer-with-testdata buf :no-flip)
      (fill-buffer-with-testdata buf)
      (fill-buffer-with-testdata (ByteBuffer/wrap one-fix))
      (let [f1 (util/byte-array-head fix 15)
            f2 (util/byte-array-head (util/byte-array-rest fix 15) 25)
            f3 (util/byte-array-rest fix 40)]
        (is (= 2 (count (into [] message-rebuilder [f1 f2 f3]))))
        (is (msg= fix-msg (first (into [] message-rebuilder [f1 f2 f3]))))
        (is (msg= fix-msg (second (into [] message-rebuilder [f1 f2 f3]))))))))

(deftest test-number-messages
  (testing "Do the messages have numbers?"
    (let [fix-msgs (map #(create-message "s1" "c1" (str %))
                        (range 1 4))]
      (is (= [1 2 3]
             (map :msg-seq-nbr
                  (into []
                        (number-messages)
                        fix-msgs))))))
  )