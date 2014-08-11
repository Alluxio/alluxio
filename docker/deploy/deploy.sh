#!/bin/bash

DEBUG=0
NUM_WORKERS=2

BASE_DIR=$(cd $(dirname $0); pwd)
TACHYON_DIR=$(cd $BASE_DIR/../..; pwd)


# if deploy on a virtual machine, id_rsa's mode may be too open, 
# ssh will then ignore this key, so we copy it to /tmp and change mode to 0600
SSH_ID_RSA_DIR=/tmp/id_rsa${RANDOM}
cp ${TACHYON_DIR}/docker/apache-hadoop-hdfs1.0.4-precise/files/id_rsa $SSH_ID_RSA_DIR
chmod 0600 $SSH_ID_RSA_DIR

NAMESERVER_IMAGE="dnsmasq-precise"
image_name="tachyon"
version="dev"

source $BASE_DIR/start_nameserver.sh
source $BASE_DIR/start_tachyon_cluster.sh

function check_root() {
  if [[ "$USER" != "root" ]]; then
    echo "please run as: sudo $0"
    exit 1
  fi
}

check_root

start_nameserver $NAMESERVER_IMAGE
wait_for_nameserver

start_master ${image_name}-master $version
sleep 5 # wait for master

start_workers ${image_name}-worker $version
sleep 10 # wait for workers to register

print_cluster_info
