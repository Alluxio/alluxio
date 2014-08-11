# Base Ubuntu Precise 12.04 LTS image
#
# Modified from amplab/apache-hadoop-hdfs-precise
FROM ubuntu:precise

# Setup a volume for data
VOLUME ["/data"]

# Set correct source list
RUN echo "deb http://archive.ubuntu.com/ubuntu precise main universe" > /etc/apt/sources.list
RUN echo "deb http://archive.ubuntu.com/ubuntu precise-updates main universe" >> /etc/apt/sources.list

# install a few other useful packages plus Open Jdk 6
RUN apt-get update && apt-get upgrade -y && apt-get install -y less openjdk-6-jre-headless net-tools vim-tiny sudo openssh-server iputils-ping python2.7

# Install Hadoop
ADD https://archive.apache.org/dist/hadoop/common/hadoop-1.0.4/hadoop_1.0.4-1_x86_64.deb /root/
RUN dpkg -i /root/hadoop_1.0.4-1_x86_64.deb 
RUN rm -rf /root/hadoop_1.0.4-1_x86_64.deb

# Docker messes up /etc/hosts and adds two entries for 127.0.0.1
# we try to recover from that by giving /etc/resolv.conf and therefore
# the nameserver priority
RUN sed -i s/"files dns"/"dns files"/ /etc/nsswitch.conf

# add Hadoop config file templates
ADD files /root/hadoop_files

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-6-openjdk-amd64
RUN echo export JAVA_HOME=/usr/lib/jvm/java-6-openjdk-amd64 >> /root/.bashrc
