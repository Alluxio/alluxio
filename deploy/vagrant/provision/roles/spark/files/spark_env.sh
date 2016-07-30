#!/usr/bin/env bash

cp /vagrant/files/workers /spark/conf/slaves

cat > /spark/conf/spark-env.sh << EOF
export SPARK_MASTER_IP=$(tail -n1 /spark/conf/slaves)
export SPARK_LOCAL_HOSTNAME=\$(hostname)
EOF
