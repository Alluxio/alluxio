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

FROM openjdk:7-jdk-alpine

ARG ALLUXIO_TARBALL=http://downloads.alluxio.org/downloads/files/1.5.0/alluxio-1.5.0-bin.tar.gz

RUN apk add --update bash && \
    rm -rf /var/cache/apk/*

ADD ${ALLUXIO_TARBALL} /opt/

# if the tarball was remote, it needs to be untarred
RUN cd /opt && \
    (if ls | grep -q ".tar.gz"; then tar -xzf *.tar.gz && rm *.tar.gz; fi) && \
    mv alluxio-* alluxio

COPY conf /opt/alluxio/conf/
COPY entrypoint.sh /

ENTRYPOINT ["/entrypoint.sh"]
