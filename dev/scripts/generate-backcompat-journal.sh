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

SCRIPTS=$(cd "$( dirname "$( readlink "$0" || echo "$0" )" )"; pwd)
. ${SCRIPTS}/../../libexec/alluxio-config.sh

classpath=${ALLUXIO_HOME}/tests/target/alluxio-tests-${VERSION}-jar-with-dependencies.jar
java -cp ${classpath} alluxio.master.backcompat.BackwardsCompatibilityJournalGenerator -o ${ALLUXIO_HOME}/tests/src/test/resources/old_journals
