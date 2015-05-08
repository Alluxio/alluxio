HADOOP_VERSION="1.0.4"

if [ ! -f /vagrant/shared/hadoop-${HADOOP_VERSION}-bin.tar.gz ]; then
    # download hadoop
    echo "Downloading hadoop ${HADOOP_VERSION} ..." 
    sudo yum install -y -q wget
    wget -q https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}-bin.tar.gz -P /vagrant/shared
fi

tar xzf /vagrant/shared/hadoop-${HADOOP_VERSION}-bin.tar.gz -C /hadoop --strip-components 1
