#!/usr/bin/env bash

PIDS=$(pgrep -f AlluxioFramework)
if [[ -n ${PIDS} ]]; then
    kill ${PIDS}
fi
