#!/bin/sh

cd /tachyon/bin
./tachyon format
./tachyon-start.sh all Mount
jps
