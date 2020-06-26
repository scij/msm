(defproject com.senacor.msm "0.1.0-SNAPSHOT"
  :description "NORM-based messaging infrastructure"
  :url "http://bitbucket.senacor.com/jschiewe/msm"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/core.async "1.2.603"]
                 [org.clojure/tools.logging "1.1.0"]
                 [org.clojure/tools.cli "1.0.194"]
                 [org.clojure/java.jmx "1.0.0"]
                 [bytebuffer "0.2.0"]
                 [mil.navy.nrl/norm "1.5.9"]
                 [me.raynes/moments "0.1.1"]
                 [org.apache.logging.log4j/log4j-api "2.13.3"]
                 [org.apache.logging.log4j/log4j-core "2.13.3"]
                 [org.clojars.scij/melee "0.2.0-SNAPSHOT"]
                 ]
  :jvm-opts ["-Djava.library.path=/usr/lib:/usr/local/lib"]
  :profiles {:test    {:resource-paths ["test-resources"]}
             :uberjar {:aot      :all}
             :docker  {:docker {:image-name "test/msm-send"
                                :dockerfile "Dockerfile.send"}
                       :plugins [[io.sarnowski/lein-docker "1.1.0"]]}
             :run     {:aot      :all
                       :jvm-opts ["-Djava.library.path=/usr/lib:/usr/local/lib"]}}
  )
