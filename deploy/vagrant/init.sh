#!/bin/sh
wget http://mirrors.gigenet.com/apache/maven/maven-3/3.2.3/binaries/apache-maven-3.2.3-bin.tar.gz
if [ $? -eq 0 ]
then
    tar -zxvf apache-maven-3.2.3-bin.tar.gz -C /opt/
    ln -f -s /opt/apache-maven-3.2.3/bin/mvn /usr/bin/mvn
    exit 0
fi
exit 1
