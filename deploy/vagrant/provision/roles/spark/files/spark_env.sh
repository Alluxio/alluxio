#!/usr/bin/env bash

cp /vagrant/files/workers /spark/conf/slaves

echo export HADOOP_CONF_DIR=/hadoop/etc/hadoop >> /spark/conf/spark-env.sh
echo export SPARK_MASTER_IP="`tail -n1 /spark/conf/slaves`" >> /spark/conf/spark-env.sh
