#!/usr/bin/env bash
#
# The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
# (the "License"). You may not use this work except in compliance with the License, which is
# available at www.apache.org/licenses/LICENSE-2.0
#
# This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied, as more fully set forth in the License.
#
# See the NOTICE file distributed with this work for information regarding copyright ownership.
#

function installLibfuse {
  git clone https://github.com/cheyang/libfuse.git
  cd libfuse
  git checkout fuse_2_9_5_customize_multi_threads_no_logging
  bash makeconf.sh
  ./configure
  make -j8
  make install
  cd ..
}

function userOperation {
  mkdir -p /journal
  chown -R $1:$2 /journal
  chmod -R g=u /journal
  mkdir /mnt/alluxio-fuse
  chown -R $1:$2 /mnt/alluxio-fuse
}

function enableDynamicUser {
  chmod -R 777 /journal; \
  chmod -R 777 /mnt; \
  # Enable user_allow_other option for fuse in non-root mode
  echo "user_allow_other" >> /etc/fuse.conf; \
}

function main {
  command=$1
  uid=$2
  gid=$3
  case $command in
    "install-libfuse")
      installLibfuse 
      ;;
    "user-operation")
      userOperation $uid $gid
      ;;
    "enable-dynamic-user")
      enableDynamicUser
      ;;
    *)
      echo "Error: dockerfile-common unknown commands"
      exit 1
  esac
}

main "$@"
