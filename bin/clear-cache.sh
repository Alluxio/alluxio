#!/usr/bin/env bash

usage="Usage: clearCache.sh"

sync; echo 3 > /proc/sys/vm/drop_caches ;