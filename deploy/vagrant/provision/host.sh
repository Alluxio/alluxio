#!/bin/sh
# uncomment it for debugging
#set -x

# patch /etc/hosts
# HDFS NN binds to the first IP that resolves the hostname.
# And thus make the default address to 127.0.0.1
# This makes other DN unable to connect to NN
# Solution is to use fully qualified domain name
cat /vagrant/files/hosts | sudo tee -a /etc/hosts

# TachyonMaster => TachyonMaster.local, TachyonWorker1 => TachyonWorker1.local, etc
sudo hostname `hostname`.local
