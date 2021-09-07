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
  git clone https://github.com/Alluxio/libfuse.git
  cd libfuse
  git checkout fuse_2_9_5_customize_multi_threads
  bash makeconf.sh
  ./configure
  make -j8
  make install
  cd ..
}

function userOperation {
  if [ $# -ne 5 ]; then
    echo "Error: user-operation: 5 parameters required. $# provided."
    exit 1
  fi
  username=$1
  groupName=$2
  uid=$3
  gid=$4
  if [ "$username" !=  "root" ] \
      && [ "$groupName" != "root" ] \
      && [ $uid -ne 0 ] \
      && [ $gid -ne 0 ]; then
    if [ "$5" = "alpine" ]; then
      addgroup --gid $gid $groupName
      adduser --system --uid $uid -G $groupName $username
      addgroup $username root
    elif [ "$5" = "centos" ]; then
      groupadd --gid $gid $groupName
      useradd --system -m --uid $uid --gid $gid $username
      usermod -a -G root $username
    else
      echo "Error: user-operation: unknown operating system or not supported."
      exit 1
    fi
    mkdir -p /journal
    chown -R $uid:$gid /journal
    chmod -R g=u /journal
    mkdir /mnt/alluxio-fuse
    chown -R $uid:$gid /mnt/alluxio-fuse
  fi
}

function enableDynamicUser {
  if [ $# -ne 1 ]; then
    echo "Error: enable-dynamic user: 1 parameter required. $# provided."
    exit 1
  fi
  if [ "$1" = "true" ]; then
    chmod -R 777 /journal
    chmod -R 777 /mnt
    # Enable user_allow_other option for fuse in non-root mode
    echo "user_allow_other" >> /etc/fuse.conf
  fi
}

function main {
  command=$1
  shift
  case $command in
    "install-libfuse")
      installLibfuse 
      ;;
    "user-operation")
      userOperation "$@"
      ;;
    "enable-dynamic-user")
      enableDynamicUser "$@"
      ;;
    *)
      echo "Error: dockerfile-common.sh unknown command."
      exit 1
  esac
}

main "$@"
