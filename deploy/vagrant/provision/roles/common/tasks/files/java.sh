#!/bin/sh

# uncomment it for debugging
#set -x

# install java
sudo yum install -y -q java-1.7.0-openjdk-devel.x86_64

echo "export JAVA_HOME=/usr/lib/jvm/java-openjdk" >> ~/.bashrc
