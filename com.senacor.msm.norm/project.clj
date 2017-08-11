(defproject com.senacor.msm "0.1.0-SNAPSHOT"
  :description "NORM-based messaging infrastructure"
  :url "http://bitbucket.senacor.com/jschiewe/msm"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/tools.logging "0.3.1"]
                 [mil.navy.nrl/norm "1.0.0"]
                 [bytebuffer "0.2.0"]
                 [org.clojure/core.async "0.3.442"]
                 [org.clojure/java.jmx "0.3.4"]
                 [log4j/log4j "1.2.17" :exclusions [javax.mail/mail
                                                    javax.jms/jms
                                                    com.sun.jmdk/jmxtools
                                                    com.sun.jmx/jmxri]]
                 ]
  :main com.senacor.msm.main.demo
  :jvm-opts ["-Djava.library.path=/usr/lib:/usr/local/lib"]
  :profiles {:test   {:resource-paths ["test-resources"]}
             :user   {:plugins [[lein-kibit "0.1.5"]]}
             :run    {:aot      :all
                      :jvm-opts ["-Djava.library.path=/usr/lib:/usr/local/lib"]}}
  )
