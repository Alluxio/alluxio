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

FROM openjdk:8-jdk-alpine

ADD http://apache.cs.utah.edu/hive/hive-2.3.6/apache-hive-2.3.6-bin.tar.gz /opt/
ADD http://apache.cs.utah.edu/hadoop/common/hadoop-2.9.2/hadoop-2.9.2.tar.gz /opt/
ADD https://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-java-5.1.48.tar.gz /opt/

RUN cd /opt && \
    (if ls | grep -q ".tar.gz"; then ls *.tar.gz | xargs -n1 tar -xzf && rm *.tar.gz; fi) && \
    ln -s apache-hive-* hive && \
    ln -s hadoop-* hadoop && \
    ln -s mysql-connector-java-* mysql-connector-java

RUN cp /opt/mysql-connector-java/*-bin.jar /opt/hive/lib

RUN apk --no-cache --update add bash libc6-compat shadow mysql mysql-client && \
    rm -rf /var/cache/apk/*

ENV PATH="/opt/hive/bin:/opt/hadoop/bin:${PATH}"

COPY initialize.sh /opt/
COPY entrypoint.sh /opt/

# initialize the hms and mysql
RUN /opt/initialize.sh

EXPOSE 9083

WORKDIR /opt/

ENTRYPOINT ["/opt/entrypoint.sh"]
