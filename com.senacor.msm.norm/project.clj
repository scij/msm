(defproject com.senacor.msm.norm "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/tools.logging "0.3.1"]
                 [mil.navy.nrl/norm "1.0.0"]]
  :jvm-opts ["-Djava.library.path=/usr/lib:/usr/local/lib"]
  :main com.senacor.msm.norm.norm-simple-test
  :profiles {:test {:resource-paths ["test-resources"]}
             :run {:aot :all
                   :resource-paths ["test-resources"]}}
  )
