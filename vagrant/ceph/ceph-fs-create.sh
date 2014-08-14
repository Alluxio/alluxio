#!/bin/sh  
if [ $# -lt 1 ]; then  
    DIR=/etc/ceph  
else  
    DIR=$1  
fi  
  
# clean up
pkill ceph-fuse 
pkill ceph-mon 
pkill ceph-osd 
pkill ceph-mds 
rm -rf $DIR  
  
# cluster wide parameters  
mkdir -p ${DIR}/log  
cat > $DIR/ceph.conf << EOF
[global]  
fsid = $(uuidgen)  
osd crush chooseleaf type = 0  
run dir = ${DIR}/run  
auth cluster required = none  
auth service required = none  
auth client required = none  
osd pool default size = 1  
EOF

export CEPH_ARGS="--conf ${DIR}/ceph.conf"  
  
# create monitor  
MON_DATA=${DIR}/mon  
mkdir -p $MON_DATA  
  
cat >> $DIR/ceph.conf << EOF  
[mon.0]  
log file = ${DIR}/log/mon.log  
chdir = ""  
mon cluster log file = ${DIR}/log/mon-cluster.log  
mon data = ${MON_DATA}  
mon addr = 127.0.0.1  
EOF
  
ceph-mon --id 0 --mkfs --keyring /dev/null  
touch ${MON_DATA}/keyring  
ceph-mon --id 0  
  
# create osd  
OSD_DATA=${DIR}/osd  
mkdir ${OSD_DATA}  
  
cat >> $DIR/ceph.conf << EOF
[osd.0]  
log file = ${DIR}/log/osd.log  
chdir = ""  
osd data = ${OSD_DATA}  
osd journal = ${OSD_DATA}.journal  
osd journal size = 100  
EOF
  
MDS_DATA=${DIR}/mds  
mkdir ${MDS_DATA}  
cat >> $DIR/ceph.conf << EOF
[mds.0]  
log file = ${DIR}/log/mds.log  
chdir = ""  
host = localhost  
EOF

OSD_ID=$(ceph osd create)  
ceph osd crush add osd.${OSD_ID} 1 root=default host=localhost  
ceph-osd --id ${OSD_ID} --mkjournal --mkfs  
ceph-osd --id ${OSD_ID}  
# check osd tree status
ceph osd tree  

ceph-mds -m 127.0.0.1:6789 -i ${OSD_ID}  

# fuse mount the filesystem  
mkdir -p /mnt/ceph  
ceph-fuse -m 127.0.0.1:6789 /mnt/ceph/  
mount

ceph osd pool set data size 3
