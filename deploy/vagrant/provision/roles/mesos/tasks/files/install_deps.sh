#!/usr/bin/env bash

set -e

# Install Mesos dependencies.
sudo yum groupinstall -y "Development Tools"
sudo yum install -y apache-maven python-devel java-1.7.0-openjdk-devel zlib-devel libcurl-devel openssl-devel cyrus-sasl-devel cyrus-sasl-md5 apr-devel subversion-devel apr-util-devel
