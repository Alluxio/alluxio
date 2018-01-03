#!/usr/bin/env bash

# add customed hosts conf to beginning of /etc/hosts
while read line; do
  sudo sed -i "1s/^/$line\n/" /etc/hosts
done < /vagrant/files/hosts
