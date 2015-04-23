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
fi