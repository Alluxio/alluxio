# Base Ubuntu Precise 12.04 LTS image
FROM apache-hadoop-hdfs-precise:1.0.4

# Where to mount the tachyon home directory on host in this container
ENV TACHYON_MOUNT /opt/tachyon_host
# Where to duplicate the tachyon home directory from $TACHYON_MOUNT in this container
ENV TACHYON_CONTAINER /opt/tachyon_container

ADD files /root/tachyon_base_files
