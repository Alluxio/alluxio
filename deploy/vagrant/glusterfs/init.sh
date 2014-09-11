wget http://download.gluster.org/pub/gluster/glusterfs/3.5/LATEST/CentOS/glusterfs-epel.repo -O /etc/yum.repos.d/glusterfs-epel.repo
yum install -y java-1.6.0-openjdk-devel.x86_64
yum install -y glusterfs-server glusterfs-client
#!/bin/sh  
echo "127.0.0.1 gfs-vm" >> /etc/hosts
mkdir -p /gfs_vol
mkdir -p /vol
service glusterd start
gluster volume create gfs_vol gfs-vm:/gfs_vol force
gluster volume start gfs_vol
mount -t glusterfs localhost:gfs_vol /vol
wget http://mirrors.gigenet.com/apache/maven/maven-3/3.0.5/binaries/apache-maven-3.0.5-bin.tar.gz
tar -zxvf apache-maven-3.0.5-bin.tar.gz -C /opt/
ln -s /opt/apache-maven-3.0.5/bin/mvn /usr/bin/mvn

echo "make sure to export JAVA_HOME=/usr/lib/jvm/java-1.6.0-openjdk-1.6.0.0.x86_64/"

echo "Now building package !!!!!!!!!!!!!"
cd /tachyon


mvn clean package -Dtest.profile=glusterfs -Dhadoop.version=2.3.0  
