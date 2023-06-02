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

set -eu

CLI_DIR=$(cd "$( dirname "$( readlink "$0" || echo "$0" )" )"; pwd)

# mapping of GOOS/GOARCH value to uname value
declare -A ALL_OS
ALL_OS["linux"]="Linux"
ALL_OS["darwin"]="Darwin"
declare -A ALL_ARCH
ALL_ARCH["amd64"]="x86_64"
ALL_ARCH["arm64"]="aarch64"

MAIN_PATH="cli/main.go"
USAGE="Usage: build-cli.sh [-a]
-a   Build executables for all OS and architecture combinations
"

main() {
  build_all="false"
  while getopts "a" opt; do
    case "${opt}" in
      a)
        build_all="true"
        ;;
      *)
        echo -e "${USAGE}" >&2
        exit 1
        ;;
    esac
  done

  cliBinDir="${CLI_DIR}/../../cli/src/alluxio.org/cli/bin"
  mkdir -p "${cliBinDir}"

  cd "${CLI_DIR}/../../cli/src/alluxio.org/"

  if [[ ${build_all} == "false" ]]; then
    GO111MODULE=on go build -o "${cliBinDir}/alluxioCli-$(uname)-$(uname -m)" "${MAIN_PATH}"
  else
    for osKey in "${!ALL_OS[@]}"; do
      for archKey in "${!ALL_ARCH[@]}"; do
        echo "Building executable for ${osKey} ${archKey}"
        GO111MODULE=on GOOS="${osKey}" GOARCH="${archKey}" go build -o "${cliBinDir}/alluxioCli-${ALL_OS[$osKey]}-${ALL_ARCH[$archKey]}" "${MAIN_PATH}"
      done
    done
  fi
}

main "$@"
