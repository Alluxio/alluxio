#!/bin/sh
# uncomment it for debugging
#set -x

# install java
sudo yum install -y -q java-1.7.0-openjdk-devel.x86_64

jbinary=`realpath /etc/alternatives/java`
jvm=$(dirname $jbinary)/..

echo "export JAVA_HOME=${jvm}" >> ~/.bashrc
