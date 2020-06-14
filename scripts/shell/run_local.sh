#!/bin/bash

cp ./configs/config.properties ./configs/alluxio-site.properties; \
export ALLUXIO_JAVA_OPTS="-Dalluxio.site.conf.dir=\${alluxio.conf.dir}/,\${user.home}/.alluxio/,/etc/alluxio/,\${alluxio.home}/configs"; \
./bin/alluxio-start.sh local Mount
