HADOOP_VERSION="2.4.1"

if [ ! -f /vagrant/shared/hadoop-${HADOOP_VERSION}.tar.gz ]
then
    # download hadoop
    echo "Downloading hadoop ${HADOOP_VERSION} ..." 
    sudo yum install -y -q wget
    wget -q http://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz -P /vagrant/shared
fi

tar xzf /vagrant/shared/hadoop-${HADOOP_VERSION}.tar.gz -C /hadoop --strip-components 1
