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

ARG ALLUXIO_TARBALL=http://downloads.alluxio.io/downloads/files/1.8.1/alluxio-1.8.1-bin.tar.gz
ARG ALLUXIO_USERNAME=alluxio
ARG ALLUXIO_GROUP=alluxio
ARG ALLUXIO_UID=1000
ARG ALLUXIO_GID=1000

RUN apk --no-cache --update add bash libc6-compat shadow && \
    rm -rf /var/cache/apk/*

ADD ${ALLUXIO_TARBALL} /opt/

# if the tarball was remote, it needs to be untarred
# use ln -s instead of mv to avoid issues with Centos (see https://github.com/moby/moby/issues/27358)
RUN cd /opt && \
    (if ls | grep -q ".tar.gz"; then tar -xzf *.tar.gz && rm *.tar.gz; fi) && \
    ln -s alluxio-* alluxio

COPY conf /opt/alluxio/conf/
COPY entrypoint.sh /

RUN addgroup -g ${ALLUXIO_GID} -S ${ALLUXIO_GROUP} && \
    adduser -u ${ALLUXIO_UID} -S ${ALLUXIO_USERNAME} -G ${ALLUXIO_GROUP} && \
    usermod -a -G root ${ALLUXIO_USERNAME} && \
    chown -R ${ALLUXIO_UID}:${ALLUXIO_GID} /opt/alluxio* && \
    chmod -R g=u /opt/alluxio*

USER ${ALLUXIO_UID}

ENTRYPOINT ["/entrypoint.sh"]
