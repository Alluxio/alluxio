#!/usr/bin/env bash

alluxio_version=`/alluxio/bin/alluxio version | cut -d':' -f2 | tr -d ' '`

perl -0777 -i -pe "s/(<artifactId>alluxio-client<\/artifactId>\n\s*)<version>.*/\1<version>${alluxio_version}<\/version>/g" /spark/core/pom.xml
