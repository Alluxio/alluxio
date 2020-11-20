#!/usr/bin/env bash
#
# The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
# (the "License"). You may not use this work except in compliance with the License, which is
# available at www.apache.org/licenses/LICENSE-2.0
#
# This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied, as more fully set forth in the License.
#
# See the NOTICE file distributed with this work for information regarding copyright ownership.
#


cp /vagrant/files/workers /spark/conf/slaves

cat > /spark/conf/spark-env.sh << EOF
export SPARK_MASTER_IP=$(tail -n1 /spark/conf/slaves)
export SPARK_LOCAL_HOSTNAME=\$(hostname)
EOF
