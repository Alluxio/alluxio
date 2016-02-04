#!/usr/bin/env bash

tachyon_version=`grep version /tachyon/pom.xml | \
                 head -n1 | tr -d ' '          | \
                 sed 's/<version>//g'          | \
                 sed 's/<\/version>//g'`

grep "TACHYON_JAR" /tachyon/libexec/tachyon-config.sh | grep "alluxio-assemblies"

if [[ "$?" == "0" ]]; then
  sed -i "s/alluxio-assemblies-.*-jar-with-dependencies.jar/alluxio-assemblies-${tachyon_version}-jar-with-dependencies.jar/g" /tachyon/libexec/tachyon-config.sh
else
  # Back compatibility
  sed -i "s/alluxio-.*-jar-with-dependencies.jar/alluxio-${tachyon_version}-jar-with-dependencies.jar/g" /tachyon/libexec/tachyon-config.sh
fi
