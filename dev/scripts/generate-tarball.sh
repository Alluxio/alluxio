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
# This script generates a tarball of the current Alluxio repo. It does the following:
# 1. Copy everything except logs/ and dev/ to a work directory
# 2. Clean out ignored files and Git metadata
# 3. Compile, using the environment variable ${BUILD_OPTS} as options to the Maven build for each client profile
# 4. Use `bin/alluxio version` to determine the right directory name, e.g. alluxio-1.2.0
# 5. Copy the generated client to the folder client/
# 6. Tar everything up and put it in dev/scripts/tarballs
#
# The --skipExtraClients flag skips building per-profile clients.
# The --deleteUnrevisioned flag cleans all unrevisioned files.
# The --hadoopProfile flag sets the hadoop profile to use.
#
# The generated tarball will be named alluxio-${VERSION}-${HADOOP_PROFILE}-bin.tar.gz. If the hadoop profile
# is not specified or specified as "default", the tarball will simply be named alluxio-${VERSION}-bin.tar.gz.
#

set -e

readonly SCRIPT_DIR=$(cd "$( dirname "$0" )"; pwd)
readonly TARBALL_DIR="${SCRIPT_DIR}/tarballs"
readonly WORK_DIR="${SCRIPT_DIR}/workdir"
readonly CLIENT_DIR="client"
# These clients need to be recompiled.
readonly EXTRA_ACTIVE_CLIENT_PROFILES=( "presto" "spark" )
# These clients can be symlinks to the default client.
readonly EXTRA_SYMLINK_CLIENT_PROFILES=( "flink" "hadoop" )
readonly HOME="${SCRIPT_DIR}/../.."
readonly BUILD_LOG="${HOME}/logs/build.log"
readonly REPO_COPY="${WORK_DIR}/alluxio"

# Cleans out previous builds and creates a clean copy of the repo.
function prepare_repo {
  cd "${SCRIPT_DIR}"
  local delete_unrevisioned=$1
  mkdir -p "${TARBALL_DIR}"
  mkdir -p "${WORK_DIR}"
  rm -rf "${REPO_COPY}"
  rsync -aq --exclude='logs' --exclude='dev' "${HOME}" "${REPO_COPY}"

  cd "${REPO_COPY}"
  if [[ "${delete_unrevisioned}" == true ]]; then
    git clean -qfdx
  else
    git clean -qfdX
  fi
  rm -rf .git .gitignore
}

function build {
  cd "${REPO_COPY}" > /dev/null
  local build_all_client_profiles=$1
  local hadoop_profile=$2
  local client_profiles_=( )
  if [[ "${build_all_client_profiles}" == true ]]; then
    client_profiles=( ${EXTRA_ACTIVE_CLIENT_PROFILES[@]} )
  fi
  client_profiles+=( "default" )
  mkdir -p "${CLIENT_DIR}"
  for profile in "${client_profiles[@]}"; do
    echo "Running build ${profile} and logging to ${BUILD_LOG}"
    local profiles_arg="-Pmesos"
    if [[ ${profile} != "default" ]]; then
      profiles_arg+=" -P${profile}"
    fi
    if [[ ${hadoop_profile} != "default" ]]; then
      profiles_arg+=" -P${hadoop_profile}"
    fi
    mvn -T 4C clean install -Dmaven.javadoc.skip=true -DskipTests -Dlicense.skip=true -Dcheckstyle.skip=true -Dfindbugs.skip=true ${profiles_arg} ${BUILD_OPTS} > "${BUILD_LOG}" 2>&1
    mkdir -p "${CLIENT_DIR}/${profile}"
    local version=$(bin/alluxio version)
    cp "core/client/runtime/target/alluxio-core-client-runtime-${version}-jar-with-dependencies.jar" "${CLIENT_DIR}/${profile}/alluxio-${version}-${profile}-client.jar"
  done
  if [[ "${build_all_client_profiles}" == true ]]; then
    for profile in "${EXTRA_SYMLINK_CLIENT_PROFILES[@]}"; do
      mkdir -p "${CLIENT_DIR}/${profile}"
      pushd "${CLIENT_DIR}/${profile}" >> /dev/null
      ln -s "../default/alluxio-${version}-default-client.jar" "alluxio-${version}-${profile}-client.jar"
      popd >> /dev/null
    done
  fi
}

function create_tarball {
  cd "${REPO_COPY}"
  local hadoop_profile=$1
  local version="$(bin/alluxio version)"
  if [[ "${hadoop_profile}" == "default" ]]; then
    local prefix="alluxio-${version}"
  else
    local prefix="alluxio-${version}-${hadoop_profile}"
  fi
  # Remove any logs generated during the build.
  rm -rf logs/*.log
  # Create the default under storage directory.
  mkdir underFSStorage
  cd ..
  rm -rf "${prefix}"
  mv "${REPO_COPY}" "${prefix}"
  local target="${TARBALL_DIR}/${prefix}-bin.tar.gz"
  gtar -czf "${target}" "${prefix}" --exclude-vcs
  echo "Generated tarball at ${target}"
}

function main {
  local build_all_client_profiles=true
  local delete_unrevisioned=false
  local hadoop_profile="default"
  while [[ "$#" > 0 ]]; do
    case $1 in
      --skipExtraClients) build_all_client_profiles=false; shift ;;
      --deleteUnrevisioned) delete_unrevisioned=true; shift ;;
      --hadoopProfile) hadoop_profile=$2; shift 2 ;;
      *) echo "Unrecognized option: $1"; exit 1 ;;
    esac
  done
  prepare_repo ${delete_unrevisioned}
  build ${build_all_client_profiles} ${hadoop_profile}
  create_tarball ${hadoop_profile}
}

main "$@"
