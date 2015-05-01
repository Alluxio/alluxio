#!/bin/bash
set -e
# clean up environment
rm -rf files

# generate key
if [ ! -f files/id_rsa ]
then
    mkdir -p files
    ssh-keygen -f files/id_rsa -t rsa -N ''
    # ssh without password
    cat files/id_rsa.pub |awk '{print $1, $2, "Generated by vagrant"}' > files/authorized_keys2
    cat files/id_rsa.pub |awk '{print $1, $2, "Generated by vagrant"}' > files/authorized_keys
fi

vagrant up --provider=aws --parallel --no-provision
# before provision, we have to ensure all machines are up and ssh usable
vagrant provision

if [[ "$?" == "0" ]]; then
    HERE=`dirname $0`
    master=`cat ${HERE}/.vagrant/provisioners/ansible/inventory/vagrant_ansible_inventory | grep Master | cut -d' ' -f2 | cut -d'=' -f2`
    purple='\033[1;35m'
    no_color='\033[0m'
    echo -e ">>> ${purple}visit $master:19999 for Tachyon Web Console${no_color} <<<"
    echo -e ">>> ${purple}visit $master:8080  for Spark   Web Console${no_color} <<<"
fi
