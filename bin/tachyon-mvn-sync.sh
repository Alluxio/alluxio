#!/bin/bash

DIR="$( cd -P "$( dirname . )" && pwd )"

bin=`cd "$( dirname "$0" )"; pwd`

# Load the Tachyon configuration
. "$bin/tachyon-config.sh"

cd $TACHYON_HOME && git pull && mvn package && ~/mesos-ec2/copy-dir . && cd $DIR