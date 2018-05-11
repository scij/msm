(ns com.senacor.msm.main.listen
  (:gen-class)
  (:require [com.senacor.msm.core.control :as control]
            [com.senacor.msm.core.util :as util]
            [com.senacor.msm.core.monitor :as monitor]
            [com.senacor.msm.core.stateless :as stateless]
            [com.senacor.msm.core.stateful :as stateful]
            [com.senacor.msm.core.topic :as topic]
            [clojure.core.async :refer [chan go go-loop mult tap untap <! >!]]
            [clojure.java.io :as io]
            [clojure.tools.cli :as cli]
            [clojure.string :as str])
  (:import (mil.navy.nrl.norm.enums NormEventType)))

(def cli-options
  [["-h" "--help"]
   ["-l" "--loopback"]
   ["-o" "--output FILE" "Output file name"]
   ["-r" "--receive MODE" "Receiver mode"
    :parse-fn #(case %
                 "stateful" :stateful
                 "stateless" :stateless
                 "topic" :topic
                 nil)
    :validate [#(contains? #{:stateful :stateless :topic} %)
               "Mode must be \"stateful\", \"stateless\" or \"topic\""]
    :default "topic"]
   ["-s" "--tos TOS" "Type of service"
    :parse-fn #(Integer/parseInt %)]
   ["-t" "--ttl HOPS" "Number of hops"
    :parse-fn #(Integer/parseInt %)]])

(defn usage
  [errors summary]
  (println (str/join "\n" errors))
  (println "Usage: listen <options> network-spec label-re")
  (println "  network-spec [interface];multicast-net:port")
  (println summary)
  (System/exit 1))

(defn print-to-file
  [file-name msg-chan]
  (go-loop [writer (io/writer file-name)
            msg (<! msg-chan)]
    (if msg
      (do
        (.write writer (prn-str msg))
        (.flush writer)
        (recur writer (<! msg-chan)))
      (.close writer))))

(defn- print-to-stdout
  [msg-chan]
  (go-loop [msg (<! msg-chan)]
    (when msg
      (println msg)
      (recur (<! msg-chan)))))

(defn start-listening
  [net-spec label options]
  (let [event-chan (chan 512)
        event-chan-m (mult event-chan)
        msg-chan (chan 128)
        instance (control/init-norm event-chan)]
    (monitor/mon-event-loop event-chan-m)
    (let [session (case (:receive options)
                    :stateless (stateless/create-session instance net-spec label event-chan-m msg-chan options)
                    :stateful  (stateful/create-session instance net-spec label event-chan-m msg-chan options)
                    (topic/create-session instance net-spec label event-chan-m msg-chan options))]
      (if (:output options)
        (print-to-file (:output options) msg-chan)
        (print-to-stdout msg-chan))
      ; TODO this will never happen. JMX Beans with actions would be a solution
      (let [ec-tap (chan 5)]
        (tap event-chan-m ec-tap)
        (util/wait-for-events ec-tap session #{NormEventType/NORM_EVENT_INVALID})
        (untap event-chan-m ec-tap))
      (control/stop-session session)
      (control/finit-norm instance))))


(defn -main
  [& args]
  (util/init-logging "listen")
  (let [opt-arg (cli/parse-opts args cli-options)
        [net-spec label] (:arguments opt-arg)]
    (when (zero? (count (:arguments opt-arg)))
      (usage ["No network specification provided"]
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
    (start-listening net-spec label (:options opt-arg))))

