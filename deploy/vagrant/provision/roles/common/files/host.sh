#!/usr/bin/env bash

# add customed hosts conf to beginning of /etc/hosts
while read line; do
  sudo sed -i "1s/^/$line\n/" /etc/hosts
done < /vagrant/files/hosts

# set hostname
IP=$(ip addr show | grep -w inet | tail -1 | grep -w inet | awk '{print $2}' | cut -d'/' -f1)
HOSTNAME=$(grep ${IP} /vagrant/files/hosts | cut -d' ' -f2)
sudo hostname ${HOSTNAME}
