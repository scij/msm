(ns com.senacor.msm.main.send
  (:require [com.senacor.msm.core.control :as control]
            [com.senacor.msm.core.message :as message]
            [com.senacor.msm.core.monitor :as monitor]
            [com.senacor.msm.core.sender :as sender]
            [com.senacor.msm.core.util :as util]
            [clojure.core.async :refer [chan go-loop mult sliding-buffer >!! <!! close!]]
            [clojure.tools.logging :as log]
            [clojure.tools.cli :as cli]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [com.senacor.msm.core.norm-api :as norm]))

(def cli-options
  [["-f" "--file FILE" "Read messages from file"]
   ["-h" "--help"]
   ["-l" "--loopback"]
   ["-r" "--repeat COUNT" "Repeat message COUNT times"
    :default 1
    :parse-fn #(Integer/parseInt %)]
   ["-a" "--autonumber" "Generate automatically numbered messages. Combine with repeat"]
   ["-s" "--tos TOS" "Type of service"
    :parse-fn #(Integer/parseInt %)]
   ["-t" "--ttl HOPS" "Number of hops"
    :parse-fn #(Integer/parseInt %)]
   ["-z" "--size MSG-SIZE" "Message buffer size"
    :default 1024
    :parse-fn #(Integer/parseInt %)]])


(defn usage
  [errors summary]
  (println (str/join "\n" errors))
  (println "Usage: listen <options> network-spec label [message]")
  (println "  network-spec [interface];multicast-net:port")
  (println summary)
  (System/exit 1))

(defn start-message-source
  [label message count autonumber out-chan]
  (log/tracef "Repeat send %s %d times" message count)
  (doseq [i (range count)]
    (log/tracef "Sending msg %d" i)
    (>!! out-chan (message/create-message label
                                          (if autonumber
                                            (str message " " i)
                                            message))))
  (close! out-chan))


(defn start-file-message-source
  [label file out-chan]
  (with-open [r (io/reader file)]
    (doseq [line (line-seq r)]
      (>!! out-chan (message/create-message label line))))
  (close! out-chan))


(defn start-sending
  [net-spec label message options]
  (let [event-chan (chan 512)
        event-chan-m (mult event-chan)
        msg-chan (chan 128 message/message-encoder)
        sync-chan (chan)
        [if-name network port] (util/parse-network-spec net-spec)
        instance (control/init-norm event-chan)
        session (control/start-session instance if-name network port options)]
    (monitor/mon-event-loop event-chan-m)
    (sender/create-sender session (norm/get-local-node-id session)
                          event-chan-m msg-chan sync-chan
                          (:size options))
    (if (:file options)
      (start-file-message-source label (:file options) msg-chan)
      (start-message-source label message (:repeat options) (:autonumber options) msg-chan))
    (<!! sync-chan)
    (log/trace "Shutdown")
    (control/finit-norm instance)
    (close! event-chan)))


(defn -main
  [& args]
  (util/init-logging "send")
  (let [opt-arg (cli/parse-opts args cli-options)
        [net-spec label message] (:arguments opt-arg)]
    (when (and (not= 2 (count (:arguments opt-arg)))
               (:file (:options opt-arg)))
      (usage ["No netspec and/or label are missing"]
             (:summary opt-arg)))
    (when (and (not= 3 (count (:arguments opt-arg)))
               (not (:file (:options opt-arg))))
      (usage ["Netspec, label and message must be provided"]
             (:summary opt-arg)))
    (when (contains? (:options opt-arg) :help)
      (usage ["Help requested"]
             (:summary opt-arg)))
    (when (:errors opt-arg)
      (usage (:errors opt-arg)
             (:summary opt-arg)))
    (when (nil? net-spec)
      (usage ["Network spec is missing"]
             (:summary opt-arg)))
    (start-sending net-spec label message (:options opt-arg))))
