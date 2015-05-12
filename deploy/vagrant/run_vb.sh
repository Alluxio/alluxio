#!/bin/bash

vagrant up --provider=virtualbox --no-provision

vagrant provision

if [[ "$?" == "0" ]]; then
 HERE=`dirname $0`
 pushd $HERE > /dev/null
 master=`tail -n 1 $HERE/files/hosts | cut -d' ' -f1`
 purple='\033[1;35m'
 no_color='\033[0m'
 echo -e ">>> ${purple}TachyonMaster public IP is ${master}${no_color} <<<"
 echo -e ">>> ${purple}visit default port of the web UI of what you deployed${no_color} <<<"
fi
