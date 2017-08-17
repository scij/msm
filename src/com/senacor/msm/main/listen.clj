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
            [clojure.string :as str]))

(def cli-options
  [["-h" "--help"]
   ["-l" "--loopback"]
   ["-i" "--node-id NODE-ID" "Node ID"
    :default (util/default-node-id)
    :parse-fn #(Integer/parseInt %)]
   ["-s" "--tos TOS" "Type of service"
    :parse-fn #(Integer/parseInt %)]
   ["-t" "--ttl HOPS" "Number of hops"
    :parse-fn #(Integer/parseInt %)]])

(defn usage
  [errors summary]
  (println (str/join "\n" errors))
  (println summary)
  (System/exit 1))

(defn start-listening
  [net-spec label options]
  (let [event-chan (chan 5)
        bytes-chan (chan 5)
        msg-chan (chan 5)
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
  (let [opt-arg (cli/parse-opts args cli-options)
        [net-spec label] (:arguments opt-arg)]
    ;; todo --help verarbeiten
    ;; todo anzahl argumente prüfen
    ;; todo default für label erzeugen
    (when (:errors opt-arg)
      (usage (:errors opt-arg)
             (:summary opt-arg)))
    (when (nil? net-spec)
      (usage ["Network spec is missing"]
             (:summary opt-arg)))
    (start-listening net-spec label (:options opt-arg))
    ))
