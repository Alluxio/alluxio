#!/bin/sh
nodes=`cat /tachyon/conf/slaves`
vol="tachyon_vol"
brick=""

# create a simple volume
for i in ${nodes[@]}
do 
    gluster peer probe ${i}
done

for i in ${nodes[@]}
do 
    if [ "x${brick}" == "x" ]
    then
        gluster volume create ${vol} ${i}:/gfs_vol force        
    else
        gluster volume add-brick ${vol} ${i}:/gfs_vol force        
    fi
    brick=${i}
done

# start volume
gluster volume start ${vol}

# mount volume
for i in ${nodes[@]}
do 
   ssh root@${i} "mount -t glusterfs localhost:${vol} /vol"
done

# config tachyon env
/bin/cp -f /tachyon/conf/tachyon-glusterfs-env.sh.template /tachyon/conf/tachyon-env.sh
