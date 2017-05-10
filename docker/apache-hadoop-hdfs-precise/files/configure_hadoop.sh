#!/bin/bash

hadoop_files=( "/root/hadoop_files/core-site.xml"  "/root/hadoop_files/hdfs-site.xml" )

function create_hadoop_directories() {
    rm -rf /root/.ssh
    mkdir /root/.ssh
    chmod go-rx /root/.ssh
    mkdir /var/run/sshd
}

function deploy_hadoop_files() {
    for i in "${hadoop_files[@]}";
    do
        filename=$(basename $i);
        cp $i /etc/hadoop/$filename;
        chown hdfs:hdfs /etc/hadoop/$filename;
        chmod 0644 /etc/hadoop/$filename;
    done
    cp /root/hadoop_files/id_rsa /root/.ssh
    chmod go-rwx /root/.ssh/id_rsa
    cp /root/hadoop_files/authorized_keys /root/.ssh/authorized_keys
    chmod go-wx /root/.ssh/authorized_keys
}		

function configure_hadoop() {
    sed -i "s/__MASTER__/$1/" /etc/hadoop/core-site.xml
    sed -i "s+JAVA_HOME=/usr/lib/jvm/java-6-sun+JAVA_HOME=$JAVA_HOME+" /etc/hadoop/hadoop-env.sh
}

function prepare_hadoop() {
    create_hadoop_directories
    deploy_hadoop_files
    configure_hadoop $1
}
