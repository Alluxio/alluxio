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

build_frameworks=true
while [[ "$#" > 0 ]]; do
  case $1 in
    --skipFrameworks) build_frameworks=false; shift;;
    *) echo "Unrecognized option: $1"; exit 1;;
  esac
done

this=$(cd "$( dirname "$0" )"; pwd)
cd "${this}"
tarball_dir="${this}/tarballs"
work_dir="${this}/workdir"
client_dir="client"
frameworks=( "flink" "presto" "spark" "hadoop" )

mkdir -p "${tarball_dir}"
mkdir -p "${work_dir}"
home="${this}/../.."
build_log="${home}/logs/build.log"
repo_copy="${work_dir}/alluxio"
rm -rf "${repo_copy}"
rsync -aq --exclude='logs' --exclude='dev' "${home}" "${repo_copy}"

pushd "${repo_copy}" > /dev/null
git clean -qfdX
mkdir -p "${client_dir}"
if [[ "${build_frameworks}" == true ]]; then
  for profile in "${frameworks[@]}"; do
    echo "Running build ${profile} and logging to ${build_log}"
    mvn -T 4C clean install -Dmaven.javadoc.skip=true -DskipTests -Dlicense.skip=true -Dcheckstyle.skip=true -Dfindbugs.skip=true -Pmesos -P${profile} ${BUILD_OPTS} > "${build_log}" 2>&1
    mkdir -p "${client_dir}/${profile}"
    version=$(bin/alluxio version)
    cp "core/client/runtime/target/alluxio-core-client-runtime-${version}-jar-with-dependencies.jar" "${client_dir}/${profile}/alluxio-${version}-${profile}-client.jar"
  done
fi
echo "Running default build and logging to ${build_log}"
mvn -T 4C clean install -Dmaven.javadoc.skip=true -DskipTests -Dlicense.skip=true -Dcheckstyle.skip=true -Dfindbugs.skip=true -Pmesos ${BUILD_OPTS} > "${build_log}" 2>&1
version="$(bin/alluxio version)"
prefix="alluxio-${version}"

cd ..
rm -rf "${prefix}"
mv alluxio "${prefix}"
target="${tarball_dir}/${prefix}.tar.gz"

gtar -czf "${target}" "${prefix}" --exclude-vcs
popd > /dev/null

echo "Generated tarball at ${target}"
