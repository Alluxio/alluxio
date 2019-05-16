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

# This script is meant for bootstrapping the Alluxio service to an EMR cluster. Arguments for the script are listed below.
# Arg 1. Download URI (ex. http://downloads.alluxio.io/downloads/files/2.0.0-preview/alluxio-2.0.0-preview-bin.tar.gz)
# Arg 2. Root UFS URI (ex. s3a://my-bucket/alluxio-emr/mount)
# Arg 3. Extra Alluxio Options. These will be appended to alluxio-site.properties. Multiple options can be specified using ';' as a delimiter
#        (ex. alluxio.user.file.writetype.default=CACHE_THROUGH;alluxio.user.file.readtype.default=CACHE)

#Create user
sudo groupadd alluxio -g 600
sudo useradd alluxio -u 600 -g 600

#Download the release
#TODO Add metadata header tag to the wget for filtering out in download metrics.
if [ -z $1]
then
  echo "No Download URL Provided. Please go to http://downloads.alluxio.io to see available release downloads."
else
  if [[ "${1}" == s3://* ]]
  then
    aws s3 cp ${1} ./
  else
    wget "${1}"
  fi
fi
RELEASE=`basename ${1}`
RELEASE_UNZIP=${RELEASE%"-bin.tar.gz"}
RELEASE_UNZIP=${RELEASE_UNZIP%".tar.gz"}

#Unpack and inflate the release tar
#TODO logic for different compression formats, s3 URIs, git URIs, etc.
sudo cp $RELEASE /opt/
sudo tar -xvf /opt/$RELEASE -C /opt/
sudo rm -R /opt/$RELEASE
sudo mv /opt/$RELEASE_UNZIP /opt/alluxio
sudo chown -R alluxio:alluxio /opt/alluxio
rm $RELEASE
sudo runuser -l alluxio -c "cp /opt/alluxio/conf/alluxio-site.properties.template /opt/alluxio/conf/alluxio-site.properties"

#Get hostnames and load into masters/workers file
EMR_CLUSTER=`jq '.jobFlowId' /mnt/var/lib/info/job-flow.json | sed -e 's/^"//' -e 's/"$//'`
HOSTLIST=`aws emr list-instances --cluster-id $EMR_CLUSTER --region us-east-1 | jq '.Instances[].PrivateDnsName' | sed -e 's/^"//' -e 's/"$//'`
MASTER=`jq '.masterHost' /mnt/var/lib/info/extraInstanceData.json | sed -e 's/^"//' -e 's/"$//' | nslookup | awk -v ip="$ip" '/name/{print substr($NF,1,length($NF)-1),ip}'`

if [ -z "$MASTER"]
then
  MASTER=`hostname`
  MASTER=$MASTER".ec2.internal"
fi

WORKERS=`printf '%s\n' "${HOSTLIST//$MASTER/}"`

sudo runuser -l alluxio -c "echo '$MASTER' > /opt/alluxio/conf/masters"
sudo runuser -l alluxio -c "echo '$WORKERS' > /opt/alluxio/conf/workers"

#Identify master
IS_MASTER=`jq '.isMaster' /mnt/var/lib/info/instance.json`

#Set up alluxio-site.properties
sudo runuser -l alluxio -c "echo 'alluxio.master.hostname=$MASTER' > /opt/alluxio/conf/alluxio-site.properties"
sudo runuser -l alluxio -c "echo 'alluxio.master.mount.table.root.ufs=$2' >> /opt/alluxio/conf/alluxio-site.properties"
sudo runuser -l alluxio -c "echo 'alluxio.worker.memory.size=20GB' >> /opt/alluxio/conf/alluxio-site.properties"
sudo runuser -l alluxio -c "echo 'alluxio.worker.tieredstore.levels=1' >> /opt/alluxio/conf/alluxio-site.properties"
sudo runuser -l alluxio -c "echo 'alluxio.worker.tieredstore.level0.alias=MEM' >> /opt/alluxio/conf/alluxio-site.properties"
sudo runuser -l alluxio -c "echo 'alluxio.worker.tieredstore.level0.dirs.path=/mnt/ramdisk' >> /opt/alluxio/conf/alluxio-site.properties"
sudo runuser -l alluxio -c "echo 'alluxio.master.security.impersonation.hive.users=*' >> /opt/alluxio/conf/alluxio-site.properties"
sudo runuser -l alluxio -c "echo 'alluxio.master.security.impersonation.yarn.users=*' >> /opt/alluxio/conf/alluxio-site.properties"
sudo runuser -l alluxio -c "echo 'alluxio.master.security.impersonation.presto.users=*' >> /opt/alluxio/conf/alluxio-site.properties"

#Inject user defined properties (semicolon separated)
IFS=';'
conf=($3)
printf "%s\n" "${conf[@]}" | sudo tee -a /opt/alluxio/conf/alluxio-site.properties

#No ssh
if [ $IS_MASTER = "true" ]
then
  sudo runuser -l alluxio -c "/opt/alluxio/bin/alluxio-start.sh master"
  sudo runuser -l alluxio -c "/opt/alluxio/bin/alluxio-start.sh job_master"
  sudo runuser -l alluxio -c "/opt/alluxio/bin/alluxio-start.sh proxy"
else
  /opt/alluxio/bin/alluxio-mount.sh SudoMount local
  while [ $MASTER_STATUS -ne "200" ]
  do
    MASTER_STATUS=`curl -s -o /dev/null -w "%{http_code}" $MASTER:19999`
    sleep 5
  done
  sudo runuser -l alluxio -c "/opt/alluxio/bin/alluxio-start.sh worker"
  sudo runuser -l alluxio -c "/opt/alluxio/bin/alluxio-start.sh job_worker"
  sudo runuser -l alluxio -c "/opt/alluxio/bin/alluxio-start.sh proxy"
fi

#Compute configs
sudo runuser -l alluxio -c "ln -s /opt/alluxio/client/*client.jar /opt/alluxio/client/alluxio-client.jar"
sudo mkdir -p /usr/lib/spark/jars/
sudo ln -s /opt/alluxio/client/alluxio-client.jar /usr/lib/spark/jars/alluxio-client.jar
sudo mkdir -p /usr/lib/presto/plugin/hive-hadoop2/
sudo ln -s /opt/alluxio/client/alluxio-client.jar /usr/lib/presto/plugin/hive-hadoop2/alluxio-client.jar
