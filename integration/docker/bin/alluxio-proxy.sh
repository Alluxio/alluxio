#!/usr/bin/env bash
#
# The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
# (the "License"). You may not use this work except in compliance with the License, which is
# available at www.apache.org/licenses/LICENSE-2.0
#
# This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied, as more fully set forth in the License.
#
# See the NOTICE file distributed with this work for information regarding copyright ownership.
#

set -e

SCRIPT_DIR=$(cd "$( dirname "$0" )"; pwd)

USAGE="Usage: alluxio-proxy

alluxio-proxy launches the Alluxio proxy in the foreground and logs to both the
console and default log file. The proxy will provide a REST interface for
interacting with the Alluxio filesystem. To configure the proxy, add
configuration properties in alluxio-site.properties or environment variables in
alluxio-env.sh."

if [[ "$#" -gt "0" ]]; then
  echo "${USAGE}"
  exit 0
fi

# Log to both the console and the proxy logs file
ALLUXIO_PROXY_LOGGER="Console,PROXY_LOGGER"

. ${SCRIPT_DIR}/../../../libexec/alluxio-config.sh

${JAVA} -cp ${ALLUXIO_SERVER_CLASSPATH} ${ALLUXIO_PROXY_JAVA_OPTS} alluxio.proxy.AlluxioProxy
