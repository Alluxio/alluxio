#!/bin/sh

REPO=/etc/yum.repos.d/epel-apache-maven.repo

if [ -f "$REPO" ]; then exit 0; fi

mkdir -p /vagrant/shared
cd /vagrant/shared

# install maven
if [ ! -f epel-apache-maven.repo ]; then
 sudo yum install -y -q wget
 wget -q http://repos.fedorapeople.org/repos/dchen/apache-maven/epel-apache-maven.repo
fi

sudo cp epel-apache-maven.repo $REPO
sudo yum install -q -y apache-maven
sudo ln -f -s /usr/share/apache-maven/bin/mvn /usr/bin/mvn
# relocate local repo to shared folder
mkdir -p ~/.m2
cat > ~/.m2/settings.xml <<EOF
<settings>
<localRepository>/vagrant/shared</localRepository>
</settings>
EOF
