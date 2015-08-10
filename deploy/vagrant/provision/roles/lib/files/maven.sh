#!/bin/sh

MAVEN_LOC=http://apache.arvixe.com/maven/maven-3/3.3.3/binaries/apache-maven-3.3.3-bin.tar.gz

mkdir -p /vagrant/shared
cd /vagrant/shared

# install maven
wget -q $MAVEN_LOC
filename=$(basename "$MAVEN_LOC")
tar xzvf $filename

IFS='-' read -a array <<< "$filename"
sudo ln -f -s "/vagrant/shared/${array[0]}-${array[1]}-${array[2]}/bin/mvn" /usr/bin/mvn

# relocate local repo to shared folder
mkdir -p ~/.m2
cat > ~/.m2/settings.xml <<EOF
<settings>
<localRepository>/vagrant/shared</localRepository>
</settings>
EOF
