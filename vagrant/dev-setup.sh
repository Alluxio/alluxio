wget http://mirrors.gigenet.com/apache/maven/maven-3/3.0.5/binaries/apache-maven-3.0.5-bin.tar.gz
tar -zxvf apache-maven-3.0.5-bin.tar.gz -C /opt/
ln -s /opt/apache-maven-3.0.5/bin/mvn /usr/bin/mvn

echo "make sure to export JAVA_HOME=/usr/lib/jvm/java-1.6.0-openjdk-1.6.0.0.x86_64/"

echo "Now building package !!!!!!!!!!!!!"
cd /tachyon

mvn clean package -Dtest.profile=cephfs -Dhadoop.version=2.3.0 -Dtachyon.underfs.hadoop.core-site=`pwd`/conf/core-site.xml
