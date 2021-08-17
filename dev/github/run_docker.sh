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

set -ex

function main {
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
    ALLUXIO_DOCKER_IMAGE="alluxio/alluxio-maven:0.0.5-jdk8"
  fi

  local run_args="--rm"

  if [ -z ${ALLUXIO_DOCKER_NO_TTY} ]
  then
    run_args+=" -it"
  fi

  if [ -n "${ALLUXIO_DOCKER_GIT_CLEAN}" ]
  then
    run_args+=" -e ALLUXIO_GIT_CLEAN=true"
  fi

  if [ -n "${ALLUXIO_DOCKER_MVN_RUNTOEND}" ]
  then
    run_args+=" -e ALLUXIO_MVN_RUNTOEND=true"
  fi

  if [ -n "${ALLUXIO_DOCKER_MVN_PROJECT_LIST}" ]
  then
    run_args+=" -e ALLUXIO_MVN_PROJECT_LIST=${ALLUXIO_DOCKER_MVN_PROJECT_LIST}"
  fi

  if [ -n "${ALLUXIO_SONAR_ARGS}" ]
  then
    # write out to a file, in case there are spaces in the args
    echo "ALLUXIO_SONAR_ARGS=${ALLUXIO_SONAR_ARGS}" > /tmp/.env
    run_args+=" --env-file /tmp/.env"
  fi

  local home="/home/jenkins"
  # Needed to run fuse tests:
  run_args+=" --cap-add SYS_ADMIN"
  run_args+=" --device /dev/fuse"
  run_args+=" --security-opt apparmor:unconfined"

  # Run as the host jenkins user so that files written to .m2 are written as jenkins.
  # Use group 0 to get certain elevated permissions.
  run_args+=" --user ${ALLUXIO_DOCKER_ID}:0"

  # Mount the local directory inside the docker container, and set it as the working directory
  run_args+=" -v $(pwd):/usr/src/alluxio"
  run_args+=" -w /usr/src/alluxio"

  # Since we're running as a user unknown to the Docker container, we need to explicitly
  # configure anything that's relative to ${HOME}.
  run_args+=" -e HOME=${home}"
  run_args+=" -v ${ALLUXIO_DOCKER_M2}:${home}/.m2"
  run_args+=" -e npm_config_cache=${home}/.npm"
  run_args+=" -e MAVEN_CONFIG=${home}/.m2"

  run_args+=" -e ALLUXIO_USE_FIXED_TEST_PORTS=true"
  run_args+=" -e ALLUXIO_PORT_COORDINATION_DIR=${home}"

  if [ -n "${ALLUXIO_COVERAGE_REPORT}" ]
  then
    run_args+=" --entrypoint=dev/github/run_coverage.sh"
  elif [ -n "${ALLUXIO_CHECKSTYLE}" ]
  then
    run_args+=" --entrypoint=dev/github/run_checks.sh"
  else
    run_args+=" --entrypoint=dev/github/run_tests.sh"
  fi

  docker run ${run_args} ${ALLUXIO_DOCKER_IMAGE} $@
}

main "$@"
