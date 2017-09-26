(ns com.senacor.msm.main.listen
  (:gen-class)
  (:require [com.senacor.msm.core.control :as control]
            [com.senacor.msm.core.receiver :as receiver]
            [com.senacor.msm.core.util :as util]
            [com.senacor.msm.core.message :as message]
            [com.senacor.msm.core.monitor :as monitor]
            [com.senacor.msm.core.norm-api :as norm]
            [com.senacor.msm.core.stateless :as stateless]
            [com.senacor.msm.core.topic :as topic]
            [clojure.core.async :refer [chan go-loop mult <! >!]]
            [clojure.tools.logging :as log]
            [clojure.tools.cli :as cli]
            [clojure.string :as str])
  (:import (org.apache.logging.log4j ThreadContext)))

(def cli-options
  [["-h" "--help"]
   ["-i" "--node-id NODE-ID" "Node ID"
    :default (util/default-node-id)
    :parse-fn #(Integer/parseInt %)]
   ["-l" "--loopback"]
   ["-o" "--output FILE" "Output file name"]
   ["-r" "--receive MODE" "Receiver mode"
    :parse-fn #(case %
                 "stateful" :stateful
                 "stateless" :stateless
                 "topic" :topic
                 nil)
    :validate [#(contains? #{:stateful :stateless :topic} %)
               "Mode must be \"stateful\", \"stateless\" or \"topic\"" ]
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

(defn- wrt
  [writer line]
  (when writer
    (binding [*out* writer]
      (println line))))

(defn start-listening
  [net-spec label options]
  (let [event-chan (chan 5)
        event-chan-m (mult event-chan)
        msg-chan (chan 5)
        instance (control/init-norm event-chan)]
    (monitor/mon-event-loop event-chan-m)
    (case (:receive options)
      :stateless (stateless/create-session instance net-spec label event-chan-m msg-chan options)
      :stateful  (log/error "Stateful sessions not yet implemented")
      (topic/create-session instance net-spec label event-chan-m msg-chan options))
    (go-loop [msg (<! msg-chan)]
      (if msg
        (do
          ; todo write to file - (:output options)
          (println msg)
          (recur (<! msg-chan)))
        (control/finit-norm instance)
        ))))

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
    (start-listening net-spec label (:options opt-arg))
    ))
