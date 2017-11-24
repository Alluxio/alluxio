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

# add jvm path to environment variables needed by JNI calling
find_libjvm()
{
  if [[ -z $JAVA_HOME ]]; then
    echo "JAVA_HOME not set"
    return 1
  fi
  platformstr=`uname`
  jvmpath=""
  if [[ "$platformstr" == 'Linux' ]]; then
    for arch in i386 amd64; do
      jvmpath="$JAVA_HOME/jre/lib/$arch/server"
      if [[ -e $jvmpath ]]; then
        break;
      else
        jvmpath=""
      fi
    done
  elif [[ "$platformstr" == 'Darwin' ]]; then
    jvmpath="$JAVA_HOME/jre/lib/server"
  fi
  if [[ -e $jvmpath ]]; then
    echo "found libjvm path at $jvmpath, add to LD_LIBRARY_PATH"
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$jvmpath
  else
    echo "cannot find JVM path, LD_LIBRARY_PATH not updated"
  fi
}

# add alluxio client assembly jar file path to environment needed by JNI calling
find_alluxio()
{
  base_dir=$ALLUXIO_HOME

  if [[ -z $base_dir ]]; then
    base_dir=$( cd "$( dirname "$0" )/.." && pwd )
    export ALLUXIO_HOME=$base_dir
  fi
    # needs to change if the alluxio jar version is different
    clientjarpath=$base_dir/assembly/client/target/alluxio-assembly-client-1.7.0-SNAPSHOT-jar-with-dependencies.jar
  if [ -f $clientjarpath ]; then
    echo "found alluxio client jar at $clientjarpath, add to CLASSPATH"
    export CLASSPATH=$CLASSPATH:$clientjarpath
  else
    echo "cannot find alluxio client jar, CLASSPATH not updated"
  fi
}

find_libjvm
find_alluxio
