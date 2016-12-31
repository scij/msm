(defproject com.senacor.msm.onramp.jms "0.1.0-SNAPSHOT"
  :description "JMS onramp processor. Reads messages from JMS and pushes them on the internal zmq bus"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [com.senacor.msm.common "0.1.0-SNAPSHOT"]
                 [org.clojure/core.async "0.2.395"]
                 [com.tibco/tibjms "7.0.1"]
                 [javax/javaee-api "7.0"]]
  :main ^:skip-aot com.senacor.msm.onramp.jms
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
