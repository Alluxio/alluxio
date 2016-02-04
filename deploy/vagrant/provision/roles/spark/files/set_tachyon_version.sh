#!/usr/bin/env bash

tachyon_version=`/tachyon/bin/tachyon version | cut -d':' -f2 | tr -d ' '`

perl -0777 -i -pe "s/(<artifactId>alluxio-client<\/artifactId>\n\s*)<version>.*/\1<version>${tachyon_version}<\/version>/g" /spark/core/pom.xml
