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

readonly SCRIPT_DIR=$(cd "$( dirname "$0" )"; pwd)
readonly GENERATE_TARBALL_SCRIPT="${SCRIPT_DIR}/generate-tarball.sh"
readonly HADOOP_PROFILES=( "default" "hadoop-1" "hadoop-2.2" "hadoop-2.3" "hadoop-2.4" "hadoop-2.5" "hadoop-2.6" "hadoop-2.7" "hadoop-2.8" )

function main {
  local build_directory="${PWD}"

  local md5_cmd
  if hash md5 2>/dev/null; then
    md5_cmd="md5"
  elif hash md5sum 2>/dev/null; then
    md5_cmd="md5sum"
  else
    echo "Could not find md5 or md5sum, md5 will not be generated"
    md5_cmd=""
  fi

  while [[ "$#" > 0 ]]; do
    case $1 in
      --directory) build_directory=$2; shift 2 ;;
      *) echo "Unrecognized option: $1"; exit 1 ;;
    esac
  done

  mkdir -p ${build_directory}
  for hadoop_profile in "${HADOOP_PROFILES[@]}"; do
    echo "Building tarball for ${hadoop_profile}"
    "${GENERATE_TARBALL_SCRIPT}" --deleteUnrevisioned --hadoopProfile ${hadoop_profile}
    local tarball="$(ls -tr ${SCRIPT_DIR}/tarballs | tail -1)"
    local full_path_tarball="${SCRIPT_DIR}/tarballs/${tarball}"
    if [[ "$(dirname ${full_path_tarball})" != "${build_directory}" ]]; then
      cp "${full_path_tarball}" "${build_directory}"
      if [[ ! -z ${md5_cmd} ]]; then
        cd "${build_directory}"
        # Need to call md5 command on only the file, otherwise the full path will be included in the md5
        ${md5_cmd} "${tarball}" > "${tarball}".md5
        cp "${tarball}.md5" "${build_directory}"
        cd -
      fi
    fi
  done
}

main "$@"
