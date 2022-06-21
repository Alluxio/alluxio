#!/usr/bin/env bash

git status >> /dev/null 2>&1
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
UPPER_CURLY_FILES=$(git grep -Il "“")
if [[ ! -z "${UPPER_CURLY_FILES}" ]]; then
    echo "The following files have upper curly quotes:"
    echo "${UPPER_CURLY_FILES}"
    exit 1
fi

LOWER_CURLY_FILES=$(git grep -Il "”")
if [[ ! -z "${LOWER_CURLY_FILES}" ]]; then
    echo "The following files have lower curly quotes:"
    echo "${LOWER_CURLY_FILES}"
    exit 1
fi
