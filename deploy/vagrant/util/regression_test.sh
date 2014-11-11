#!/bin/bash

# This test covers vagrant aws, openstack, and virtualbox providers
# it uses hdfs2 as ufs and provision Tachyon clusters on these providers.
# a successful deployment will yield status 0.

PROVIDERS=("aws" "openstack" "virtualbox")
CONFIGS=("init.yml.aws" "init.yml.os" "init.yml.hdfs2")
CMDS=("sh ./run_aws.sh" "sh ./run_openstack.sh" "vagrant up")
TIMEOUT=1200s # 20 minutes for timeout
for (( i = 0 ; i < ${#PROVIDERS[@]} ; i++ )) 
do
    # clean up enviornment
    rm -rf files shared
    # print them out for sanity check
    echo -n "Provider: "${PROVIDERS[i]} " Config: "${CONFIGS[i]} " CMD: "${CMDS[i]}
    # create a cluster configuration
    ln -fs ${CONFIGS[i]} init.yml
    # provision the cluster
    ${CMDS[i]} > buildlog.${PROVIDERS[i]} 2>&1
    # get the status, error 124 is timeout.
    echo " Status: "$?
    # clean up the cluster
    vagrant destroy -f > /dev/null 2>&1
done