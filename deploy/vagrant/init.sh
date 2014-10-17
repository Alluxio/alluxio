#!/bin/sh
set -x
mkdir -p /vagrant/shared
cd /vagrant/shared

# install maven
if [ ! -f apache-maven-3.2.3-bin.tar.gz ]
then
    wget -q http://mirrors.gigenet.com/apache/maven/maven-3/3.2.3/binaries/apache-maven-3.2.3-bin.tar.gz 
fi

if [ -f apache-maven-3.2.3-bin.tar.gz ]
then
    tar -zxf apache-maven-3.2.3-bin.tar.gz -C /opt/
    ln -f -s /opt/apache-maven-3.2.3/bin/mvn /usr/bin/mvn
    # relocate local repo to shared folder
    mkdir -p ~/.m2
    cat > ~/.m2/settings.xml <<EOF
<settings>
  <localRepository>/vagrant/shared</localRepository>
</settings>
EOF
    # ssh config
    mkdir -p ~/.ssh
    src="/tachyon/deploy/vagrant/files"
    if [ ! -d ${src} ]
    then
        mkdir ${src}
        # ensure we have ssh-keygen rpm installed
        sudo yum install -y -q openssh
        # generate key
        ssh-keygen -f ${src}/id_rsa -t rsa -N ''
        # ssh without password
        cat ${src}/id_rsa.pub |awk '{print $1, $2, "root@vagrant"}' > ${src}/authorized_keys2
    fi
    files=('authorized_keys2' 'id_rsa' 'id_rsa.pub')
    for f in ${files[@]}
    do
      cp ${src}/${f} ~/.ssh
    done
    cat > ~/.ssh/config <<EOF
Host *
   StrictHostKeyChecking no
   UserKnownHostsFile=/dev/null
EOF

    chmod 600 ~/.ssh/*
    # install java
    sudo yum install -y -q java-1.6.0-openjdk-devel.x86_64
    jvm="/usr/lib/jvm/java-1.6.0-openjdk.x86_64"
    echo "export JAVA_HOME=${jvm}" >> ~/.bashrc
    exit 0
fi

echo "Failed to download maven"
exit 1
