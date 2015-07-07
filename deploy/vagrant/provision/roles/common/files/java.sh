#!/bin/sh
# uncomment it for debugging
#set -x

# install java
sudo yum install -y -q java-1.6.0-openjdk-devel.x86_64
jvm="/usr/lib/jvm/java-1.6.0-openjdk.x86_64"
echo "export JAVA_HOME=${jvm}" >> ~/.bashrc
