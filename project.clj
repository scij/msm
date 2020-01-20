(defproject com.senacor.msm "0.1.0-SNAPSHOT"
  :description "NORM-based messaging infrastructure"
  :url "http://bitbucket.senacor.com/jschiewe/msm"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/core.async "0.7.559"]
                 [org.clojure/tools.logging "0.5.0"]
                 [org.clojure/tools.cli "0.4.2"]
                 [org.clojure/java.jmx "0.3.4"]
                 [bytebuffer "0.2.0"]
                 [mil.navy.nrl/norm "1.5.9"]
                 [me.raynes/moments "0.1.1"]
                 [org.apache.logging.log4j/log4j-api "2.13.0"]
                 [org.apache.logging.log4j/log4j-core "2.13.0"]
                 [org.clojars.scij/melee "0.2.0-SNAPSHOT"]
                 ]
  :jvm-opts ["-Djava.library.path=/usr/lib:/usr/local/lib"]
  :profiles {:test    {:resource-paths ["test-resources"]}
             :qa      {:plugins [[lein-kibit "0.1.8"]
                                 [jonase/eastwood "0.3.7"]
                                 [lein-cloverage "1.1.2"]
                                 [lein-test-out "0.3.1"]
                                 [lein-nvd "1.3.1"]
                                 [lein-ancient "0.6.15"]]}
             :uberjar {:aot      :all}
             :docker  {:docker {:image-name "test/msm-send"
                                :dockerfile "Dockerfile.send"}
                       :plugins [[io.sarnowski/lein-docker "1.1.0"]]}
             :run     {:aot      :all
                       :jvm-opts ["-Djava.library.path=/usr/lib:/usr/local/lib"]}}
  )
