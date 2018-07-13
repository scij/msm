(defproject com.senacor.msm "0.1.0-SNAPSHOT"
  :description "NORM-based messaging infrastructure"
  :url "http://bitbucket.senacor.com/jschiewe/msm"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.clojure/core.async "0.4.474"]
                 [org.clojure/tools.logging "0.4.0"]
                 [org.clojure/tools.cli "0.3.5"]
                 [org.clojure/java.jmx "0.3.4"]
                 [bytebuffer "0.2.0"]
                 [mil.navy.nrl/norm "1.5.6"]
                 [me.raynes/moments "0.1.1"]
                 [org.apache.logging.log4j/log4j-api "2.8.2"]
                 [org.apache.logging.log4j/log4j-core "2.8.2"]
                 [org.clojars.scij/melee "0.2.0-SNAPSHOT"]
                 ]
  :jvm-opts ["-Djava.library.path=/usr/lib:/usr/local/lib"]
  :profiles {:test    {:resource-paths ["test-resources"]}
             :qa      {:plugins [[lein-kibit "0.1.6"]
                                 [lein-cloverage "1.0.10"]
                                 [lein-test-out "0.3.1"]
                                 [lein-ancient "0.6.15"]]}
             :uberjar {:aot      :all}
             :docker  {:docker {:image-name "test/msm-send"
                                :dockerfile "Dockerfile.send"}
                       :plugins [[io.sarnowski/lein-docker "1.1.0"]]}
             :run     {:aot      :all
                       :jvm-opts ["-Djava.library.path=/usr/lib:/usr/local/lib"]}}
  )
