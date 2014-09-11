cat >> /etc/yum.repos.d/ceph.repo << EOF
[ceph-noarch]
name=Ceph noarch packages
baseurl=http://ceph.com/rpm-firefly/rhel6/noarch
enabled=1
gpgcheck=1
type=rpm-md
gpgkey=https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/release.asc
EOF
sudo yum update && sudo yum install ceph-deploy
yum install -y java-1.6.0-openjdk-devel.x86_64
yum install -y ceph
yum install -y ceph-release
yum install -y ceph-deploy
yum install -y ceph-common
yum install -y ceph-devel
yum install -y ceph-fuse libcephfs1.x86_64 rbd-fuse 

echo "Now installing the java bindings" 

yum install -y cephfs-java.x86_64  
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
wget http://mirrors.gigenet.com/apache/maven/maven-3/3.0.5/binaries/apache-maven-3.0.5-bin.tar.gz
tar -zxvf apache-maven-3.0.5-bin.tar.gz -C /opt/
ln -s /opt/apache-maven-3.0.5/bin/mvn /usr/bin/mvn

cd /tachyon

mvn clean package -Dtest.profile=cephfs -Dhadoop.version=2.3.0
