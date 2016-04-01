#!/bin/bash

# maven package's location
MAVEN_LOC=http://apache.cs.utah.edu/maven/maven-3/3.3.9/binaries/apache-maven-3.3.9-bin.tar.gz

# maven package's name
MAVEN_FN=$(basename "$MAVEN_LOC")

mkdir -p /vagrant/shared
cd /vagrant/shared

# maven's dir after uncompression
IFS='-' read -a array <<< "$MAVEN_FN"
MAVEN_DIR=/vagrant/shared/${array[0]}-${array[1]}-${array[2]}

if [ -d ${MAVEN_DIR} ] && [ $(readlink -f /usr/bin/mvn) == "${MAVEN_DIR}/bin/mvn" ]; then
  echo "Maven 3 is already installed."
  exit 0
fi

# install maven
wget -q ${MAVEN_LOC}
tar xzvf ${MAVEN_FN}
sudo ln -f -s "${MAVEN_DIR}/bin/mvn" /usr/bin/mvn

# relocate local repo to shared folder
mkdir -p ~/.m2
cat > ~/.m2/settings.xml << EOF
<settings>
<localRepository>/vagrant/shared</localRepository>
</settings>
EOF
