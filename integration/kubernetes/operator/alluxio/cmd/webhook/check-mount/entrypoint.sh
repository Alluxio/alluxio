#!/bin/sh
set -xe

function log() {
    echo $(date +"[%Y%m%d %H:%M:%S]: ") $1
}

cp /check-mount.sh /target/check-mount.sh

# nsenter -t 1 --mnt --uts --ipc --net --pid -- bash /tmp/check-mount.sh