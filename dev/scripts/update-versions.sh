#!/usr/bin/env bash

# This script is designed so that someone can update all versions for releases
# using a single command.

# Debugging
# set -x
set -e # exit if a command fails
set -u # unbound variable is an error

HELP=$(cat <<EOF
Usage: update-version.sh [-s] <old version> <new version>

    <old version>    The version to replace throughout the codebase
    <new version>    The new version in the codebase

    -s               Skips updating maven pom versions. This should mainly be
                     used when cutting a final release where pom versions are
                     updated by the maven release plugin.


EOF
)

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
    pushd integration/kubernetes
    ./helm-generate.sh all
    popd
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

    update_libexec "$_old" "$_new"
    update_docs "$_old" "$_new"
    update_k8s "$_old" "$_new"

    exit 0
}

main "$@"