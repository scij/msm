# Installation

## Install to build

This section contains information on how to prepare you system to develop or
modify the MSM implementation.

### NORM

The standard NORM binary packages as delivered via homebrew do not contain the
Java integration. To build NORM from source download the source code tarball from [https://downloads.pf.itd.nrl.navy.mil/norm/]
and unpack it. Make sure you have a C++ compiler installed (or get clang) and
your JAVA_HOME is set. Then configure the package with

    ./waf --build-java configure
    
Next run the build job by calling

    ./waf
    
and install the shared libraries.

    ./waf install
    
Finally install the NORM Java API to your local Maven repository.

    mvn install:install-file -Dfile=build/norm.jar -DgroupId=mil.navy.nrl -DartifactId=norm -Dversion=1.5.6 -Dpackaging=jar
    
## Install to run

