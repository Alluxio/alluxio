#!/bin/bash

if [[ -n "$BASH_VERSION" ]]; then
    BIN_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
elif [[ -n "$ZSH_VERSION" ]]; then
    BIN_DIR="$( cd "$( dirname "${(%):-%x}" )" && pwd )"
else
    echo "Please, launch your scripts from zsh or bash only" >&2
    exit 1
fi

###########################
# Customize the following #
###########################
TACHYON_CLIENT_W_DEP_JAR=${BIN_DIR}/../libs/tachyon-client-0.8.2-jar-with-dependencies.jar
#DEBUG_FLAG="-d"
DEBUG_FLAG=""

# The user that runs Tachyon-fuse must have R/W rights on the mount point
MOUNT_POINT=/mnt/tachyon

TACHYON_MASTER_HOST=localhost
TACHYON_MASTER_PORT=19998
TACHYON_MASTER_ADDRESS=tachyon://${TACHYON_MASTER_HOST}:${TACHYON_MASTER_PORT}


###################################################
# Probably you don't want to change the following #
###################################################
LOG4J_CONF=file://${BIN_DIR}/log4j.properties

JAVA_OPTS+="
  -server
  -Xms1G
  -Xmx1G
  -cp ${BIN_DIR}/../target/tachyon-fuse-assembly-0.1-SNAPSHOT.jar:${TACHYON_CLIENT_W_DEP_JAR}
  -Dlog4j.configuration=${LOG4J_CONF}
"

TACHYON_JAVA_OPTS+="
  -Dtachyon.logger.type=tachyon.client
  -Dtachyon.master.port=${TACHYON_MASTER_PORT}
  -Dtachyon.master.hostname=${TACHYON_MASTER_HOST}
  -Dtachyon.master.address=${TACHYON_MASTER_ADDRESS}
"

FUSE_MAX_WRITE=131072

java ${JAVA_OPTS} ${TACHYON_JAVA_OPTS} com.ibm.ie.tachyon.fuse.TachyonFuse ${DEBUG_FLAG}\
  -m ${MOUNT_POINT} -t ${TACHYON_MASTER_ADDRESS} -o big_writes -o max_write=$FUSE_MAX_WRITE

