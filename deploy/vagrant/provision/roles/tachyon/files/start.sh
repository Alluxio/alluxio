#!/usr/bin/env bash

set -e

cd /tachyon/bin
./tachyon format
./tachyon-start.sh all SudoMount
