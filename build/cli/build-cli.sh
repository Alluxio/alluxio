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

# tuple of GOOS, GOARCH, and combined uname value for binary name
OS_ARCH_TUPLES=(
"linux amd64 Linux-x86_64"
"darwin amd64 Darwin-x86_64"
"linux arm64 Linux-aarch64"
"darwin arm64 Darwin-aarch64"
)

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
    for val in "${OS_ARCH_TUPLES[@]}"; do
      IFS=" " read -r -a tuple <<< "${val}"
      echo "Building executable for ${tuple[0]} ${tuple[1]}"
      GO111MODULE=on GOOS="${tuple[0]}" GOARCH="${tuple[1]}" go build -o "${cliBinDir}/alluxioCli-${tuple[2]}" "${MAIN_PATH}"
    done
  fi
}

main "$@"
