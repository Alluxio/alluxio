#!/bin/bash

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

find_alluxio()
{
  base_dir=$ALLUXIO_HOME

  if [[ -z $base_dir ]];
   base_dir=$( cd "$( dirname "$0"/.. )" && pwd )

  fi
  clientjarpath=$base_dir/assembly/client/target/alluxio-assembly-client-1.7.0-SNAPSHOT-jar-with-dependencies.jar
  echo "heheeeeee"
echo $clientjarpath
  if [ -f $clientjarpath ]; then
    echo "found alluxio client jar at $clientjarpath, add to CLASSPATH"
    export CLASSPATH=$CLASSPATH:$clientjarpath
  else
    echo "cannot find alluxio client jar, CLASSPATH not updated"
  fi
}

find_libjvm
find_alluxio
