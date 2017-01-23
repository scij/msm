(defproject com.senacor.msm.common "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/core.async "0.2.395"]
                 [org.zeromq/jeromq "0.3.6"]
                 [org.zeromq/cljzmq "0.1.4" :exclusions [org.zeromq/jzmq]]
                 ;;[org.zeromq/cljzmq "0.1.4"]
                 [org.clojure/tools.logging "0.3.1"]
                 [log4j/log4j "1.2.17" :exclusions [javax.mail/mail
                                                    javax.jms/jms
                                                    com.sun.jmdk/jmxtools
                                                    com.sun.jmx/jmxri]]]
  ;;:jvm-opts ["-Djava.library.path=/usr/lib:/usr/local/lib"]
  :profiles {:test {:resource-paths ["test-resources"]}}
  :repositories [["sonatype" {:url "https://oss.sonatype.org/content/repositories/releases"
                              :update :always}]])
