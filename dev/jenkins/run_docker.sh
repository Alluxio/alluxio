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

if [ -z "${ALLUXIO_DOCKER_ID}" ]
then
  ALLUXIO_DOCKER_ID="$(id -u)"
fi
if [ -z "${ALLUXIO_DOCKER_M2}" ]
then
  ALLUXIO_DOCKER_M2="${HOME}/.m2"
fi
if [ -z "${ALLUXIO_DOCKER_IMAGE}" ]
then
  ALLUXIO_DOCKER_IMAGE="alluxio/alluxio-maven:0.0.3"
fi

docker run \
       --rm \
       --cap-add SYS_ADMIN \
       --device /dev/fuse \
       --security-opt apparmor:unconfined \
       --user ${ALLUXIO_DOCKER_ID}:0 \
       -v $(pwd):/usr/src/alluxio \
       -w /usr/src/alluxio \
       -v ${ALLUXIO_DOCKER_M2}:/home/jenkins/.m2 \
       -e npm_config_cache=/home/jenkins/.npm \
       -e MAVEN_CONFIG=/home/jenkins/.m2 \
       -e ALLUXIO_USE_FIXED_TEST_PORTS=true \
       ${ALLUXIO_DOCKER_IMAGE} \
       dev/jenkins/build.sh

# Needed to run fuse tests:
# --cap-add SYS_ADMIN
# --device /dev/fuse
# --security-opt apparmor:unconfined

# Run as the host jenkins user so that files written to .m2 are written as jenkins.
# Use group 0 to get certain elevated permissions.
# --user $(id -u jenkins):0

# Mount the local directory inside the docker container, and set it as the working directory
# -v $(pwd):/usr/src/alluxio
# -w /usr/src/alluxio

# Since we're running as a user unknown to the Docker container, we need to explicitly
# configure anything that's relative to ${HOME}.
# -v ${ALLUXIO_DOCKER_M2}:/home/jenkins/.m2
# -e npm_config_cache=/home/jenkins/.npm
# -e MAVEN_CONFIG=/home/jenkins/.m2
