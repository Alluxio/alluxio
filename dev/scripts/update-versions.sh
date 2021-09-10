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

# This script is designed so that someone can update all versions for releases
# using a single command.

# Debugging
# set -x
set -e # exit if a command fails
set -u # unbound variable is an error

HELP=$(cat <<EOF
Usage: update-version.sh [-s] <old version> <new version>

    <old version>    The version to replace throughout the codebase
                     (i.e. 2.2.0-SNAPSHOT)
    <new version>    The new version in the codebase (i.e. 2.2.0-RC1)

    -s               Skips updating maven pom versions. This should mainly be
                     used when cutting a final release where pom versions are
                     updated by the maven release plugin.


EOF
)

# Arguments:
#  $1: old version
#  $2: new version
function update_dataproc() {
    perl -pi -e "s/${1}/${2}/g" integration/dataproc/alluxio-dataproc.sh
}

# Arguments:
#  $1: old version
#  $2: new version
function update_emr() {
    perl -pi -e "s/${1}/${2}/g" integration/emr/alluxio-emr.sh
}

# Arguments:
#  $1: old version
#  $2: new version
function update_poms() {
    find . -name pom.xml | xargs -t -n 1 perl -pi -e "s/${1}/${2}/g"
}

# Arguments:
#  $1: old version
#  $2: new version
function update_libexec() {
    perl -pi -e "s/${1}/${2}/g" libexec/alluxio-config.sh
}

# Arguments:
#  $1: old version
#  $2: new version
function update_readme() {
    perl -pi -e "s/${1}/${2}/g" README.md
}

# Arguments:
#  $1: old version
#  $2: new version
function update_docs() {

    local current_branch="$(git rev-parse --abbrev-ref HEAD)"
    perl -pi -e "s/${1}/${2}/g" docs/_config.yml

    if [[ "${current_branch}" != "master" ]]; then
        local major_version_regex="([0-9]+\.[0-9]+)\."
        # regex is unquoted on purpose
        if [[ "${2}" =~ ${major_version_regex} ]]; then
            local match="${BASH_REMATCH[1]}"
            # .* matches everything after it on that line
            perl -pi -e \
            "s/ALLUXIO_MAJOR_VERSION.*/ALLUXIO_MAJOR_VERSION: ${match}/g" \
                docs/_config.yml

        fi
    fi
}

# Arguments:
#  $1: old version
#  $2: new version
function update_k8s() {
    perl -pi -e "s/${1}/${2}/g" \
        integration/kubernetes/helm-chart/alluxio/values.yaml
}

function update_dockerfiles() {
    perl -pi -e "s/${1}/${2}/g" integration/docker/Dockerfile
    perl -pi -e "s/${1}/${2}/g" integration/docker/Dockerfile-dev
}


function main() {
    local skip_maven="false"

    while getopts "sh" o; do
        case "${o}" in
            s)
                skip_maven="true"
                shift
                ;;
            h)
                echo "${HELP}"
                exit 0
                ;;
        esac
    done

    if [[ "$#" -lt 2 ]]; then
        echo "Arguments '<old version>' and '<new version>' must be provided."
        echo "${HELP}"
        exit 1
    fi

    local _old="${1}"
    local _new="${2}"

    if [[ "${skip_maven}" == "false" ]]; then
        update_poms "$_old" "$_new"
    fi

    update_dataproc "$_old" "$_new"
    update_emr "$_old" "$_new"
    update_libexec "$_old" "$_new"
    update_readme "$_old" "$_new"
    update_docs "$_old" "$_new"
    update_k8s "$_old" "$_new"
    update_dockerfiles "$_old" "$_new"

    exit 0
}

main "$@"
