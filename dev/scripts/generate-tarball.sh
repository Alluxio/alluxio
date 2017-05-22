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

#
# This script generates a tarball of the current Alluxio commit. It does the following:
# 1. Copy everything except logs/ and dev/ to a work directory
# 2. Clean out ignored files
# 3. Compile, using the environment variable ${BUILD_OPTS} as options to the Maven build for each compute framework
# 4. Use `bin/alluxio version` to determine the right directory name, e.g. alluxio-1.2.0
# 5. Copy the generated client to the folder client/framework/
# 6. Tar everything up and put it in dev/scripts/tarballs
#
# Example: BUILD_OPTS="-Phadoop-2.7" ./generate-tarball.sh
#
# The --skipFrameworks flag may be used to avoid building per-framework clients.
#

set -e

readonly SCRIPT_DIR=$(cd "$( dirname "$0" )"; pwd)
readonly TARBALL_DIR="${SCRIPT_DIR}/tarballs"
readonly WORK_DIR="${SCRIPT_DIR}/workdir"
readonly CLIENT_DIR="client"
readonly FRAMEWORKS=( "flink" "presto" "spark" "hadoop" )
readonly HOME="${SCRIPT_DIR}/../.."
readonly BUILD_LOG="${HOME}/logs/build.log"
readonly REPO_COPY="${WORK_DIR}/alluxio"

# Cleans out previous builds and creates a clean copy of the repo.
function prepare_repo {
  cd "${SCRIPT_DIR}"
  mkdir -p "${TARBALL_DIR}"
  mkdir -p "${WORK_DIR}"
  rm -rf "${REPO_COPY}"
  rsync -aq --exclude='logs' --exclude='dev' "${HOME}" "${REPO_COPY}"
  cd "${REPO_COPY}"
  git clean -qfdX
}

function build_framework_clients {
  cd "${REPO_COPY}" > /dev/null
  mkdir -p "${CLIENT_DIR}"
  for profile in "${FRAMEWORKS[@]}"; do
    echo "Running build ${profile} and logging to ${BUILD_LOG}"
    mvn -T 4C clean install -Dmaven.javadoc.skip=true -DskipTests -Dlicense.skip=true -Dcheckstyle.skip=true -Dfindbugs.skip=true -Pmesos -P${profile} ${BUILD_OPTS} > "${BUILD_LOG}" 2>&1
    mkdir -p "${CLIENT_DIR}/${profile}"
    version=$(bin/alluxio version)
    cp "core/client/runtime/target/alluxio-core-client-runtime-${version}-jar-with-dependencies.jar" "${CLIENT_DIR}/${profile}/alluxio-${version}-${profile}-client.jar"
  done
}

function build_default {
  cd "${REPO_COPY}"
  echo "Running default build and logging to ${BUILD_LOG}"
  mvn -T 4C clean install -Dmaven.javadoc.skip=true -DskipTests -Dlicense.skip=true -Dcheckstyle.skip=true -Dfindbugs.skip=true -Pmesos ${BUILD_OPTS} > "${BUILD_LOG}" 2>&1
}

function create_tarball {
  cd "${REPO_COPY}"
  version="$(bin/alluxio version)"
  prefix="alluxio-${version}"
  cd ..
  rm -rf "${prefix}"
  mv "${REPO_COPY}" "${prefix}"
  target="${TARBALL_DIR}/${prefix}.tar.gz"
  gtar -czf "${target}" "${prefix}" --exclude-vcs
  echo "Generated tarball at ${target}"
}

function main {
  local build_frameworks
  build_frameworks=true
  while [[ "$#" > 0 ]]; do
    case $1 in
      --skipFrameworks) build_frameworks=false; shift ;;
      *) echo "Unrecognized option: $1"; exit 1 ;;
    esac
  done
  prepare_repo
  if [[ "${build_frameworks}" == true ]]; then
    build_framework_clients
  fi
  build_default
  create_tarball
}

main "$@"
