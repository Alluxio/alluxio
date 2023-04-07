#!/bin/bash
#
# The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
# (the "License"). You may not use this work except in compliance with the License, which is
# available at www.apache.org/licenses/LICENSE-2.0
#
# This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied, as more fully set forth in the License.
#
# See the NOTICE file distributed with this work for information regarding copyright ownership.
#


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
