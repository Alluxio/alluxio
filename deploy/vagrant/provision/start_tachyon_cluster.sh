#!/bin/sh

cd /tachyon/bin
./tachyon format
./tachyon-start.sh all SudoMount
jps
