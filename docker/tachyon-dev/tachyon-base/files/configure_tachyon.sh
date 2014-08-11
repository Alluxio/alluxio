#!/bin/bash

source /root/hadoop_files/configure_hadoop.sh

function create_tachyon_directories() {
  create_hadoop_directories

  dir_needed=( "bin" "conf" "libexec" "core/target" "core/src/main/webapp" )
  for d in ${dir_needed[@]}; do
    src=${TACHYON_MOUNT}/${d}
    dst=${TACHYON_CONTAINER}/${d}
    # Keep the directory structure
    mkdir -p `dirname $dst` # cp --parent not supported on Mac
    echo "copy from $src to $dst"
    cp -r $src $dst
  done
  #echo "change $TACHYON_CONTAINER owner:group to hdfs:hdfs"
  #sudo chown -R hdfs:hdfs ${TACHYON_CONTAINER}
}

function deploy_tachyon_files() {
  deploy_hadoop_files
  rm -f "$TACHYON_CONTAINER/conf/tachyon-env.sh"
  confs=( "log4j.properties" "slaves" "tachyon-env.sh.template" )
  for conf in ${confs[@]}; do
    local command="cp /root/tachyon_base_files/$conf $TACHYON_CONTAINER/conf"
    echo $command
    $command
  done
}

function configure_tachyon() {
  configure_hadoop $1
}

function prepare_tachyon() {
  create_tachyon_directories
  deploy_tachyon_files
  configure_tachyon $1
}