(defproject com.senacor.msm.norm "0.1.0-SNAPSHOT"
  :description "NORM-based messaging infrastructure"
  :url "http://bitbucket.senacor.com/jschiewe/msm"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/tools.logging "0.3.1"]
                 [mil.navy.nrl/norm "1.0.0"]
                 [bytebuffer "0.2.0"]
                 [gloss "0.2.6"]
                 [org.clojure/core.async "0.3.442"]
                 ]
  :main com.senacor.msm.norm.norm-simple-test
  :profiles {:test   {:resource-paths ["test-resources"]}
             :proto3 {:protobuf-version "3.2.0"}
             :run    {:aot      :all
                      :jvm-opts ["-Djava.library.path=/usr/lib:/usr/local/lib"]}}
  )
