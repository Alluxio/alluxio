#!/usr/bin/env bash

tachyon_version=`grep version /tachyon/pom.xml | \
                 head -n1 | tr -d ' '          | \
                 sed 's/<version>//g'          | \
                 sed 's/<\/version>//g'`

sed -i "s/tachyon-assemblies-.*-jar-with-dependencies.jar/tachyon-assemblies-${tachyon_version}-jar-with-dependencies.jar/g" /tachyon/libexec/tachyon-config.sh
