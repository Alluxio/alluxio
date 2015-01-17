#!/bin/sh
# uncomment it for debugging
#set -x

mkdir -p /vagrant/shared
cd /vagrant/shared
sudo yum install -y -q wget 

# install maven
if [ ! -f epel-apache-maven.repo ]
then
    wget -q http://repos.fedorapeople.org/repos/dchen/apache-maven/epel-apache-maven.repo
fi

if [ -f epel-apache-maven.repo ]
then
    sudo cp epel-apache-maven.repo /etc/yum.repos.d/epel-apache-maven.repo
    sudo yum install -q -y apache-maven
    sudo ln -f -s /usr/share/apache-maven/bin/mvn /usr/bin/mvn
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
    mkdir -p ${src}
    if [ ! -f ${src}/id_rsa ]
    then
        # ensure we have ssh-keygen rpm installed
        sudo yum install -y -q openssh
        # generate key
        ssh-keygen -f ${src}/id_rsa -t rsa -N ''
        # ssh without password
        cat ${src}/id_rsa.pub |awk '{print $1, $2, "Generated by vagrant"}' >> ${src}/authorized_keys2
        cat ${src}/id_rsa.pub |awk '{print $1, $2, "Generated by vagrant"}' >> ${src}/authorized_keys
    fi
    files=('authorized_keys2' 'authorized_keys' 'id_rsa' 'id_rsa.pub')
    for f in ${files[@]}
    do
      cp ${src}/${f} ~/.ssh
    done
    cat >> ~/.ssh/config <<EOF
Host *
   StrictHostKeyChecking no
   UserKnownHostsFile=/dev/null
EOF
    chmod 600 ~/.ssh/*

    # patch /etc/hosts
    # HDFS NN binds to the first IP that resolves the hostname. 
    # And thus make the default address to 127.0.0.1 
    # This makes other DN unable to connect to NN
    # Solution is to use fully qualified domain name
    cat ${src}/hosts >> /etc/hosts

    # install java
    sudo yum install -y -q java-1.6.0-openjdk-devel.x86_64
    jvm="/usr/lib/jvm/java-1.6.0-openjdk.x86_64"
    echo "export JAVA_HOME=${jvm}" >> ~/.bashrc
    exit 0
fi

echo "Failed to download maven"
exit 1
