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

# Setup CSI
# Make sure any changes to CSI installation are made in Dockerfile-dev as well
FROM golang:1.15.13-alpine AS csi-dev
ENV GO111MODULE=on
RUN mkdir -p /alluxio-csi
COPY ./csi /alluxio-csi
RUN cd /alluxio-csi && \
    CGO_ENABLED=0 go build -o /usr/local/bin/alluxio-csi

# We have to do an ADD to put the tarball into extractor, then do a COPY with chown into final
# ADD then chown in two steps will double the image size
#   See - https://stackoverflow.com/questions/30085621/why-does-chown-increase-size-of-docker-image
#       - https://github.com/moby/moby/issues/5505
#       - https://github.com/moby/moby/issues/6119
# ADD with chown doesn't chown the files inside tarball
#   See - https://github.com/moby/moby/issues/35525
FROM alpine:3.10.2 AS alluxio-extractor
# Note that downloads for *-SNAPSHOT tarballs are not available.
ARG ALLUXIO_TARBALL=http://downloads.alluxio.io/downloads/files/2.9.0-SNAPSHOT/alluxio-2.9.0-SNAPSHOT-bin.tar.gz
# (Alert):It's not recommended to set this Argument to true, unless you know exactly what you are doing
ARG ENABLE_DYNAMIC_USER=false

ADD ${ALLUXIO_TARBALL} /opt/
# Remote tarball needs to be untarred. Local tarball is untarred automatically.
# Use ln -s instead of mv to avoid issues with Centos (see https://github.com/moby/moby/issues/27358)
RUN cd /opt && \
    (if ls | grep -q ".tar.gz"; then tar -xzf *.tar.gz && rm *.tar.gz; fi) && \
    ln -s alluxio-* alluxio

RUN if [ ${ENABLE_DYNAMIC_USER} = "true" ] ; then \
       chmod -R 777 /opt/* ; \
    fi

FROM centos:7 as build_java11
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk

FROM build_java11 AS final

ADD dockerfile-common.sh /

# Install libfuse2 and libfuse3. Libfuse2 setup is modified from cheyang/fuse2:ubuntu1604-customize to be applied on centOS
WORKDIR /

ENV MAX_IDLE_THREADS "64"

RUN \
    yum update -y && yum upgrade -y && \
    yum install -y ca-certificates pkgconfig wget udev git && \
    yum install -y gcc gcc-c++ make cmake gettext-devel libtool autoconf && \
    yum clean all && \
    ./dockerfile-common.sh install-libfuse2 && \
    yum install -y fuse3 fuse3-devel fuse3-lib

ENV LD_LIBRARY_PATH "/usr/local/lib:${LD_LIBRARY_PATH}"

ARG ALLUXIO_USERNAME=alluxio
ARG ALLUXIO_GROUP=alluxio
ARG ALLUXIO_UID=1000
ARG ALLUXIO_GID=1000
ARG ENABLE_DYNAMIC_USER=true

# Add Tini for Alluxio helm charts (https://github.com/Alluxio/alluxio/pull/12233)
# - https://github.com/krallin/tini
ENV TINI_VERSION v0.18.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-static /usr/local/bin/tini
RUN chmod +x /usr/local/bin/tini

RUN \
    yum install -y java-11-openjdk-devel java-11-openjdk && \
    yum install -y unzip vim && \
    yum clean all

# Disable JVM DNS cache (https://github.com/Alluxio/alluxio/pull/9452)
RUN echo "networkaddress.cache.ttl=0" >> ${JAVA_HOME}/conf/security/java.security

# Add the following for native libraries needed by rocksdb
ENV LD_LIBRARY_PATH /lib64:${LD_LIBRARY_PATH}

# If Alluxio user, group, gid, and uid aren't root|0, create the alluxio user and set file permissions accordingly
RUN ./dockerfile-common.sh user-operation ${ALLUXIO_USERNAME} ${ALLUXIO_GROUP} ${ALLUXIO_UID} ${ALLUXIO_GID} centos

# Docker 19.03+ required to expand variables in --chown argument
# https://github.com/moby/buildkit/pull/926#issuecomment-503943557
COPY --from=alluxio-extractor --chown=${ALLUXIO_USERNAME}:${ALLUXIO_GROUP} /opt /opt/
COPY --chown=${ALLUXIO_USERNAME}:${ALLUXIO_GROUP} conf /opt/alluxio/conf/
COPY --chown=${ALLUXIO_USERNAME}:${ALLUXIO_GROUP} entrypoint.sh /
COPY --from=csi-dev /usr/local/bin/alluxio-csi /usr/local/bin/

RUN ./dockerfile-common.sh enable-dynamic-user ${ENABLE_DYNAMIC_USER}

USER ${ALLUXIO_UID}

WORKDIR /opt/alluxio

ENV PATH="/opt/alluxio/bin:${PATH}"

ENTRYPOINT ["/entrypoint.sh"]
