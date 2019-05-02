#!/bin/bash

#Create user and download release
sudo groupadd alluxio -g 600
sudo useradd alluxio -u 600 -g 600
wget http://downloads.alluxio.io/downloads/files/2.0.0-preview/alluxio-2.0.0-preview-bin.tar.gz
sudo cp alluxio-*.tar.gz /opt/
sudo tar -xvf /opt/alluxio-*.tar.gz -C /opt/
sudo rm -R /opt/alluxio-*.tar.gz
sudo mv /opt/alluxio-* /opt/alluxio
sudo chown -R alluxio:alluxio /opt/alluxio
sudo rm -R /opt/alluxio-*.tar.gz
rm alluxio-*.tar.gz
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
sudo runuser -l alluxio -c "echo 'alluxio.underfs.address=s3a://my-bucket/emr/alluxio/' >> /opt/alluxio/conf/alluxio-site.properties"
sudo runuser -l alluxio -c "echo 'alluxio.worker.memory.size=1GB' >> /opt/alluxio/conf/alluxio-site.properties"
sudo runuser -l alluxio -c "echo 'alluxio.worker.tieredstore.levels=1' >> /opt/alluxio/conf/alluxio-site.properties"
sudo runuser -l alluxio -c "echo 'alluxio.worker.tieredstore.level0.alias=MEM' >> /opt/alluxio/conf/alluxio-site.properties"
sudo runuser -l alluxio -c "echo 'alluxio.worker.tieredstore.level0.dirs.path=/mnt/ramdisk' >> /opt/alluxio/conf/alluxio-site.properties"
sudo runuser -l alluxio -c "echo 'alluxio.master.security.impersonation.hive.users=*' >> /opt/alluxio/conf/alluxio-site.properties"
sudo runuser -l alluxio -c "echo 'alluxio.master.security.impersonation.presto.users=*' >> /opt/alluxio/conf/alluxio-site.properties"

#No ssh
if [ $IS_MASTER = "true" ]
then
  sudo runuser -l alluxio -c "/opt/alluxio/bin/alluxio-start.sh master"
  sudo runuser -l alluxio -c "/opt/alluxio/bin/alluxio-start.sh proxy"
else
  /opt/alluxio/bin/alluxio-mount.sh SudoMount local
  while [ $MASTER_STATUS -ne "200" ]
  do
    MASTER_STATUS=`curl -s -o /dev/null -w "%{http_code}" $MASTER:19999`
    sleep 5
  done
  sudo runuser -l alluxio -c "/opt/alluxio/bin/alluxio-start.sh worker"
  sudo runuser -l alluxio -c "/opt/alluxio/bin/alluxio-start.sh proxy"
fi

#Compute configs
sudo runuser -l alluxio -c "ln -s /opt/alluxio/client/*client.jar /opt/alluxio/client/alluxio-client.jar"
sudo mkdir -p /usr/lib/presto/plugin/hive-hadoop2/
sudo ln -s /opt/alluxio/client/alluxio-client.jar /usr/lib/presto/plugin/hive-hadoop2/alluxio-client.jar
