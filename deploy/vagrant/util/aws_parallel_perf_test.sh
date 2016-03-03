#!/bin/bash
N_INSTANCE_BEG=2
N_INSTANCE_END=5

ROOT=`dirname $0`
pushd ${ROOT}/.. > /dev/null

if [[ `uname -a` == Darwin* ]]; then
 SED="sed -i ''" 
else
 SED="sed -i"
fi

for n in `seq ${N_INSTANCE_BEG} ${N_INSTANCE_END}`; do
 # sed will break symbolic link by default, and sed on mac behaves
 # differently from sed on linux, this is a walk around
 real_init_yml=`readlink init.yml`
 ${SED} "/ - /d" ${real_init_yml}
 ${SED} "s/^Total:.*/Total: $n/g" ${real_init_yml}
 ln -fs ${real_init_yml} init.yml
 for i in `seq 1 ${n}`; do
  echo "    - 172.31.35.$i" >> init.yml
 done
 echo "$n instances"
 time bash run_aws.sh
 vagrant destroy -f
 # wait for aws to shutdown
 sleep 60
done

popd > /dev/null
