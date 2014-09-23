#!/bin/sh

# install maven
wget -q http://mirrors.gigenet.com/apache/maven/maven-3/3.2.3/binaries/apache-maven-3.2.3-bin.tar.gz 

if [ $? -eq 0 ]
then
    tar -zxf apache-maven-3.2.3-bin.tar.gz -C /opt/
    ln -f -s /opt/apache-maven-3.2.3/bin/mvn /usr/bin/mvn

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
    # create tachyon env
    #FIXME be more under fs specific
    cp /tachyon/tachyon-env.sh.template /tachyon/tachyon-env.sh 
fi

echo "Failed to download maven"
exit 1
