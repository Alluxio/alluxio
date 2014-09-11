#!/bin/bash

function check_root() {
  if [[ "$USER" != "root" ]]; then
    echo "please run as: sudo $0"
    exit 1
  fi
}  

function kill_all_tachyon_container() {
  docker ps | grep "tachyon" | awk '{print $1}' | xargs docker kill
}

function kill_nameserver() {
  docker ps | grep "dnsmasq" | awk '{print $1}' | xargs docker kill
}

check_root
echo "killing all tachyon containers"
kill_all_tachyon_container
echo "killing nameserver"
kill_nameserver
echo "remaining running containers"
docker ps

