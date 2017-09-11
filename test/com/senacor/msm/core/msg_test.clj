(ns com.senacor.msm.core.msg-test
  (:require [com.senacor.msm.core.message :refer :all]
            [com.senacor.msm.core.util :as util]
            [clojure.test :refer :all]
            [bytebuffer.buff :as bb]
            [clojure.core.async :refer [<!! >!! <! >! chan to-chan timeout close!]])
  (:import (java.nio ByteBuffer)))

(def fix-msg (create-message "label" "uuid" "payload"))

(def fix-buflen (message-length "label" "uuid" "payload"))

(defn bytes-eq?
  [^bytes b1 ^bytes b2]
   (if (= (count b1) (count b2))
     (every? identity (map = b1 b2))
     false)
  )

(deftest test-encode
  (testing "just encode"
    (let [fxb ^bytes (Message->bytes fix-msg)]
      (is fxb)
      (is (= fix-buflen (count fxb)))
      ))
  (testing "encode and inspect"
    (let [fxb (ByteBuffer/wrap (Message->bytes fix-msg))]
      (is (= fix-buflen (.remaining fxb)))
      (is (= 77 (bb/take-byte fxb)))
      (is (= 88 (bb/take-byte fxb)))
      (is (= 1  (bb/take-byte fxb)))
      (is (= 0  (bb/take-byte fxb)))
      (is (= 11 (bb/take-short fxb)))
      (is (= 7  (bb/take-int fxb)))
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
      (is (= 0 (.remaining fxb)))
      ))
  )

(defn fill-buffer-with-testdata
  "Fill buf with predefined test data and flip the buffer.
  When no-flip is set, leave the buffer unchanged"
  [^ByteBuffer buf & no-flip]
  (doto buf
    (bb/put-byte 77)
    (bb/put-byte 88)
    (bb/put-byte 1)
    (bb/put-byte 0)
    (bb/put-short 11)
    (bb/put-int 7)
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
      (is (= (String. ^"[B" (.array buf))
             (String. (Message->bytes fix-msg)))))
    )
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
      (is (= 18 (.remaining buf)))
      ))
  (testing "buffer io with flip"
    (let [buf (bb/byte-buffer 20)]
      (is (= 20 (.remaining buf)))
      (bb/put-byte buf 1)
      (bb/put-byte buf 2)
      (.flip buf)
      (is (= 2 (.remaining buf)))
      ))
  (testing "buffer out with flip"
    (let [buf (bb/byte-buffer 20)]
      (is (= 20 (.remaining buf)))
      (bb/put-byte buf (byte \a))
      (bb/put-byte buf (byte \b))
      (.flip buf)
      (bb/put-byte buf (byte \c))
      (bb/put-byte buf (byte \d))
      (is (= 0 (.remaining buf))) ;; flip has set the limit to 2
      (is (= "cd" (String. (.array buf) 0 2)))
      ))
  )

(deftest test-async-io
  (testing "send and close"
    (let [c (chan 1)]
      (>!! c "hi")
      (close! c)
      (is (= "hi" (<!! c)))
      (is (nil? (<!! c))))))

(deftest test-take-string
  (testing "take string"
    (let [buf (bb/byte-buffer 5)]
      (bb/put-byte buf 4)
      (bb/put-byte buf (byte \a))
      (bb/put-byte buf (byte \b))
      (bb/put-byte buf (byte \c))
      (bb/put-byte buf (byte \d))
      (.flip buf)
      (is (= (take-string buf) "abcd"))))
  (testing "take string length exceeded"
    (let [buf (bb/byte-buffer 5)]
      (bb/put-byte buf 5)
      (bb/put-byte buf (byte \a))
      (bb/put-byte buf (byte \b))
      (bb/put-byte buf (byte \c))
      (bb/put-byte buf (byte \d))
      (.flip buf)
      (is (= (take-string buf) "abcd"))))
  (testing "empty buffer"
    (let [buf (bb/byte-buffer 1)]
      (bb/put-byte buf 0)
      (.flip buf)
      (bb/take-byte buf)
      (is (= "" (take-string buf))))))

(deftest test-process-message
  (testing "One message, one buffer"
    (let [fix (.array (fill-buffer-with-testdata (bb/byte-buffer fix-buflen) :no-flip))
          out-chan (chan 1)]
      (process-message start-state fix out-chan)
      (is (= fix-msg (<!! out-chan)))
      ))
  (testing "one message, short buffer"
    (let [fix (fill-buffer-with-testdata (bb/byte-buffer fix-buflen) :no-flip)
          out-chan (chan 1)
          arr1 (util/byte-array-head (.array fix) 10)]
      (is (= parse-var-header (:parse-fn (process-message start-state
                                                          arr1
                                                          out-chan))))
      ))
  (testing "one message, split buffer"
    (let [fix (fill-buffer-with-testdata (bb/byte-buffer fix-buflen) :no-flip)
          out-chan (chan 1)
          arr1 (util/byte-array-head (.array fix) 10)
          arr2 (util/byte-array-rest (.array fix) 10)
          state (process-message start-state arr1 out-chan)]
      (is (= parse-fixed-header (:parse-fn (process-message state
                                                            arr2
                                                            out-chan))))
      (is (= fix-msg (<!! out-chan)))
      ))
  (testing "two messages, one buffer"
    (let [fix (fill-buffer-with-testdata (bb/byte-buffer (* 2 fix-buflen)) :no-flip)
          out-chan (chan 2)]
      (fill-buffer-with-testdata fix :flip)
      (is (= parse-fixed-header (:parse-fn (process-message start-state
                                                            (.array fix)
                                                            out-chan))))
      (is (= fix-msg (<!! out-chan)))
      (is (= fix-msg (<!! out-chan)))
      ))
  (testing "one message with some rest"
    (let [fix (fill-buffer-with-testdata (bb/byte-buffer (+ fix-buflen 2)) :no-flip)
          out-chan (chan 1)]
      (bb/put-byte fix (byte \M))
      (bb/put-byte fix (byte \X))
      (.flip fix)
      (is (= "MX" (String. ^bytes (:rest-arr (process-message start-state
                                                              (.array fix)
                                                              out-chan)))))
      (is (= fix-msg (<!! out-chan)))
      ))
  )

(defn split-buffer-test
  [pos]
  (testing (str "at pos " pos)
    (let [fix (fill-buffer-with-testdata (bb/byte-buffer 28) :no-flip)
          arr1 (util/byte-array-head (.array fix) pos)
          arr2 (util/byte-array-rest (.array fix) pos)
          out-chan (chan 1)
          in-chan (chan 2)]
      (>!! in-chan arr1)
      (>!! in-chan arr2)
      (bytes->Messages in-chan out-chan)
      out-chan)
    )
  )

(deftest test-message-receiver
  (testing "receive one message, one buffer"
    (let [fix (fill-buffer-with-testdata (bb/byte-buffer 28) :no-flip)
          in-chan (chan 1)
          out-chan (chan 1)]
      (is (= out-chan (bytes->Messages in-chan out-chan)))
      (>!! in-chan (.array fix))
      (is (= fix-msg (<!! out-chan)))
      ))
  (testing "one message, split in two parts"
    (testing "at section boundary"
      (is (= fix-msg (<!! (split-buffer-test 10)))))
    (testing "in fixed header"
      (is (= fix-msg (<!! (split-buffer-test 5)))))
    (testing "in var header"
      (is (= fix-msg (<!! (split-buffer-test 14)))))
    (testing "in uuid"
      (is (= fix-msg (<!! (split-buffer-test 19)))))
    (testing "in payload"
      (is (= fix-msg (<!! (split-buffer-test 24)))))
    )
  (testing "two messages, one buffer"
    (let [fix (fill-buffer-with-testdata (bb/byte-buffer (* 2 fix-buflen)) false)
          in-chan (chan 2)
          out-chan (chan 2)]
      (fill-buffer-with-testdata fix :no-flip)
      (bytes->Messages in-chan out-chan)
      (>!! in-chan (.array fix))
      (is (= fix-msg (<!! out-chan)))
      (is (= fix-msg (<!! out-chan)))
      )
    )
  (testing "two messages, two buffers"
    (let [fix (fill-buffer-with-testdata (bb/byte-buffer fix-buflen) :no-flip)
          in-chan (chan 2)
          out-chan (chan 2)]
      (bytes->Messages in-chan out-chan)
      (>!! in-chan (.array fix))
      (>!! in-chan (.array fix))
      (is (= fix-msg (<!! out-chan)))
      (is (= fix-msg (<!! out-chan)))
      )
    )
  (testing "close channel"
    (let [fix (fill-buffer-with-testdata (bb/byte-buffer fix-buflen) :no-flip)
          in-chan (chan 2)
          out-chan (chan 2)]
      (bytes->Messages in-chan out-chan)
      (>!! in-chan (.array fix))
      (close! in-chan)
      (is (= fix-msg (<!! out-chan)))
      (is (nil? (<!! out-chan)))
      )
    )
  )

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
      (is (label-match #"com.senacor.*" fix))))
  )

(deftest test-skip-message
  (testing "isolated skipping test"
    (let [bb (bb/byte-buffer (* 2 fix-buflen))]
      (bb/put-byte bb (byte \a))
      (bb/put-byte bb (byte \b))
      (bb/put-byte bb (byte \c))
      (let [fix (fill-buffer-with-testdata bb)]
        (is (zero? (.position fix)))
        (is (.hasRemaining fix))
        (is (= start-state (skip-to-next-msg-prefix {} fix nil)))
        (is (= 3 (.position fix))))))
  (testing "isolated skipping test with single M before message"
    (let [bb (bb/byte-buffer (* 2 fix-buflen))]
      (bb/put-byte bb (byte \a))
      (bb/put-byte bb msg-prefix-m)
      (bb/put-byte bb (byte \c))
      (let [fix (fill-buffer-with-testdata bb)]
        (is (zero? (.position fix)))
        (is (.hasRemaining fix))
        (is (= start-state (skip-to-next-msg-prefix {} fix nil)))
        (is (= 3 (.position fix))))))
  (testing "no new message in buffer"
    (let [bb (bb/byte-buffer 4)]
      (bb/put-byte bb (byte \a))
      (bb/put-byte bb (byte \b))
      (bb/put-byte bb (byte \c))
      (bb/put-byte bb (byte \d))
      (.flip bb)
      (is (zero? (.position bb)))
      (is (.hasRemaining bb))
      (is (= {} (skip-to-next-msg-prefix {} bb nil)))
      (is (not (.hasRemaining bb)))))
  (testing "Msg - Trunk Msg - Msg"
    ; expected to skip the message following the truncated one.
    (let [bb (bb/byte-buffer (* fix-buflen 3))
          fix (fill-buffer-with-testdata bb :no-flip)
          trunc (fill-buffer-with-testdata (bb/byte-buffer fix-buflen) :flip)
          in-chan (chan 2)
          out-chan (chan 4)]
      (.put fix (util/byte-array-head (.array trunc) 10))
      (fill-buffer-with-testdata fix)
      (bytes->Messages in-chan out-chan)
      (>!! in-chan (.array fix))
      (close! in-chan)
      (is (= fix-msg (<!! out-chan)))
      (is (nil? (<!! out-chan)))
      ))
  (testing "Msg - Trunk Msg - Msg - Msg"
    ; should find the fourth message
    (let [bb (bb/byte-buffer (* fix-buflen 20))
          fix (fill-buffer-with-testdata bb :no-flip)
          in-chan (chan 2)
          out-chan (chan 2)]
      (bb/with-buffer fix
                      (bb/put-byte (byte \a))
                      (bb/put-byte (byte \a)))
      (fill-buffer-with-testdata fix :no-flip)
      (fill-buffer-with-testdata fix)
      (bytes->Messages in-chan out-chan)
      (>!! in-chan (.array fix))
      (close! in-chan)
      (is (= fix-msg (<!! out-chan)))
      (is (= fix-msg (<!! out-chan)))
      (is (nil? (<!! out-chan))))
    ))

(deftest test-parse-var-header
  "Broken message and parse var header actually hits a new message header"
  (let [bb (bb/byte-buffer fix-buflen)
        fix (fill-buffer-with-testdata bb :flip)]
    (is (= {:parse-fn skip-to-next-msg-prefix,
            :complete? false,
            :valid? false}
           (parse-var-header {:payload-length 0} fix nil)))))

