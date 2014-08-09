sudo yum update && sudo yum install ceph-deploy
yum install -y java-1.6.0-openjdk-devel.x86_64
yum install -y ceph
yum install -y ceph-release
yum install -y ceph-deploy
yum install -y ceph-common
yum install -y ceph-devel
yum install -y ceph-fuse
yum install -y ceph-fuse libcephfs1.x86_64 rbd-fuse 

echo "Now installing the java bindings" 

yum install -y cephfs-java.x86_64  
