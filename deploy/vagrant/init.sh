#!/bin/sh

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
    yum install -y -q java-1.6.0-openjdk-devel.x86_64
    echo "export JAVA_HOME=/usr/lib/jvm/java-1.6.0-openjdk-1.6.0.0.x86_64/" >> ~/.bashrc
    exit 0
fi

echo "Failed to download maven"
exit 1
