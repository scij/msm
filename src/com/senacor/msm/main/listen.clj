(ns com.senacor.msm.main.listen
  (:gen-class)
  (:require [com.senacor.msm.core.control :as control]
            [com.senacor.msm.core.receiver :as receiver]
            [com.senacor.msm.core.util :as util]
            [com.senacor.msm.core.message :as message]
            [com.senacor.msm.core.monitor :as monitor]
            [com.senacor.msm.core.norm-api :as norm]
            [clojure.core.async :refer [chan go-loop mult <! >!]]
            [clojure.tools.logging :as log]
            [clojure.tools.cli :as cli]
            [clojure.string :as str])
  (:import (org.apache.logging.log4j ThreadContext)))

(defn init-logging
  [app-name]
  (ThreadContext/put "app" app-name))

(def cli-options
  [["-h" "--help"]
   ["-l" "--loopback"]
   ["-i" "--node-id NODE-ID" "Node ID"
    :default (util/default-node-id)
    :parse-fn #(Integer/parseInt %)]
   ["-o" "--output FILE" "Output file name"]
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
        bytes-chan (chan 5)
        msg-chan (chan 5 (filter (partial message/label-match (re-pattern label))))
        event-chan-m (mult event-chan)
        [if-name network port] (util/parse-network-spec net-spec)
        instance (control/init-norm event-chan)
        session (control/start-session instance network port options)]
    (when if-name
      (norm/set-multicast-interface session if-name))
    (monitor/mon-event-loop event-chan-m)
    (receiver/create-receiver session event-chan-m bytes-chan)
    (message/bytes->Messages bytes-chan msg-chan)
    (go-loop [msg (<! msg-chan)]
      (if msg
        (do
          (println msg)
          (recur (<! msg-chan)))
        (control/finit-norm instance)
        ))))

(defn -main
  [& args]
  (init-logging "listen")
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
