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

# For production use, we use alpine as base image to minimize alluxio image size.
FROM alpine:3.10.2 AS final

ADD dockerfile-common.sh /

# Install libfuse2 and libfuse3. Libfuse2 setup is modified from cheyang/fuse2:ubuntu1604-customize to be applied on alpine
WORKDIR /

ENV MAX_IDLE_THREADS "64"

# wget depends on ca-certificates
RUN \
  apk update && apk upgrade && \
  apk add --no-cache ca-certificates wget bash && \
  apk add --no-cache --virtual .build-deps \
  build-base pkgconfig eudev git gcc make cmake gettext-dev libtool autoconf automake && \
  ./dockerfile-common.sh install-libfuse2 && \
  rm -rf libfuse && \
  apk add fuse3=3.2.6-r1 && \
  apk del --no-cache .build-deps

ENV LD_LIBRARY_PATH "/usr/local/lib:${LD_LIBRARY_PATH}"

ENV ALLUXIO_USERNAME=alluxio
ENV ALLUXIO_GROUP=alluxio
ENV ALLUXIO_UID=1000
ENV ALLUXIO_GID=1000
ENV ENABLE_DYNAMIC_USER=true

# Add Tini for Alluxio helm charts (https://github.com/Alluxio/alluxio/pull/12233)
# - https://github.com/krallin/tini
ENV TINI_VERSION v0.18.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-static /usr/local/bin/tini
RUN chmod +x /usr/local/bin/tini

RUN apk add --no-cache openjdk8 openjdk8-jre-lib unzip vim

ENV JAVA_HOME /usr/lib/jvm/java-1.8-openjdk

# Disable JVM DNS cache (https://github.com/Alluxio/alluxio/pull/9452)
RUN echo "networkaddress.cache.ttl=0" >> ${JAVA_HOME}/jre/lib/security/java.security

# Add the following for native libraries needed by rocksdb
ENV LD_LIBRARY_PATH /lib64:${LD_LIBRARY_PATH}

# Docker 19.03+ required to expand variables in --chown argument
# https://github.com/moby/buildkit/pull/926#issuecomment-503943557
COPY --from=alluxio-extractor /opt /opt/
COPY conf /opt/alluxio/conf/
COPY entrypoint.sh /
COPY --from=csi-dev /usr/local/bin/alluxio-csi /usr/local/bin/

RUN ./dockerfile-common.sh enable-dynamic-user ${ENABLE_DYNAMIC_USER}

WORKDIR /opt/alluxio

ENV PATH="/opt/alluxio/bin:${PATH}"

ENTRYPOINT ["/entrypoint.sh"]
