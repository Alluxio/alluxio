#!/bin/bash

set -ex

[ -z ${ConditionPathIsMountPoint} ] && ConditionPathIsMountPoint=/alluxio-fuse

count=0
# while ! mount | grep alluxio | grep  $ConditionPathIsMountPoint | grep -v grep
while ! cat /target/1/mounts  |grep $ConditionPathIsMountPoint | grep fuse.alluxio-fuse
do
    sleep 1
    count=`expr $count + 1`
    if test $count -eq 6000
    then
        echo "timed out!"
        exit 1
    fi
done

echo "succeed in checking mount point $ConditionPathIsMountPoint"