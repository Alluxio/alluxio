#!/usr/bin/env bash

git status
if [[ $? -ne 0 ]]; then
    # we aren't in a git repo, no need to run this check
    exit 0
fi

WINDOWS_FILES=$(git grep -Il "$")
if [[ ! -z "${WINDOWS_FILES}" ]]; then
    echo "The following files have windows line endings:"
    echo "${WINDOWS_FILES}"
    exit 1
fi
