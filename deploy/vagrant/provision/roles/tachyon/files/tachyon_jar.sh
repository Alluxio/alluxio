#!/usr/bin/env bash

tachyon_version=`grep version /tachyon/pom.xml | \
                 head -n1 | tr -d ' '          | \
                 sed 's/<version>//g'          | \
                 sed 's/<\/version>//g'`

grep "TACHYON_JAR" /tachyon/libexec/tachyon-config.sh | grep "tachyon-assemblies"

if [[ "$?" == "0" ]]; then
  sed -i "s/tachyon-assemblies-.*-jar-with-dependencies.jar/tachyon-assemblies-${tachyon_version}-jar-with-dependencies.jar/g" /tachyon/libexec/tachyon-config.sh
else
  # Back compatibility
  sed -i "s/tachyon-.*-jar-with-dependencies.jar/tachyon-${tachyon_version}-jar-with-dependencies.jar/g" /tachyon/libexec/tachyon-config.sh
fi
