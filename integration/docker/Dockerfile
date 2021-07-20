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

# Use a multi-stage build to include artifacts without using a chown
# chown in bash nearly doubles the image size of the docker image.
# See:
# - https://stackoverflow.com/questions/30085621/why-does-chown-increase-size-of-docker-image
# - https://github.com/moby/moby/issues/5505
# - https://github.com/moby/moby/issues/6119
FROM alpine:3.10.2
# Note that downloads for *-SNAPSHOT tarballs are not available
ARG ALLUXIO_TARBALL=http://downloads.alluxio.io/downloads/files/2.7.0-SNAPSHOT/alluxio-2.7.0-SNAPSHOT-bin.tar.gz
# (Alert):It's not recommended to set this Argument to true, unless you know exactly what you are doing
ARG ENABLE_DYNAMIC_USER=false

ADD ${ALLUXIO_TARBALL} /opt/
# if the tarball was remote, it needs to be untarred
# use ln -s instead of mv to avoid issues with Centos (see https://github.com/moby/moby/issues/27358)
RUN cd /opt && \
    (if ls | grep -q ".tar.gz"; then tar -xzf *.tar.gz && rm *.tar.gz; fi) && \
    ln -s alluxio-* alluxio

RUN if [ ${ENABLE_DYNAMIC_USER} = "true" ] ; then \
       chmod -R 777 /opt/* ; \
    fi

# The following setup is modified from cheyang/fuse2:ubuntu1604-customize to be applied on alpine
WORKDIR /

# fuse3 requires meson 0.42, available from stretch-backports
# wget depends on ca-certificates

RUN \
  apk update && app upgrade && \
  apk add --no-cache build-base ca-certificates pkgconfig wget eudev git && \
  apk add --no-cache gcc make cmake gettext-dev libtool autoconf automake

RUN \
   git clone https://github.com/cheyang/libfuse.git && \
   cd libfuse && \
   git checkout fuse_2_9_5_customize_multi_threads_no_logging

#ADD libfuse /libfuse

WORKDIR "/libfuse"

RUN bash makeconf.sh && \
  ./configure && \
  make -j8 && \
  make install

ENV LD_LIBRARY_PATH "/usr/local/lib:${LD_LIBRARY_PATH}"

ENV MAX_IDLE_THREADS "64"

WORKDIR /

ARG ALLUXIO_USERNAME=alluxio
ARG ALLUXIO_GROUP=alluxio
ARG ALLUXIO_UID=1000
ARG ALLUXIO_GID=1000
ARG ENABLE_DYNAMIC_USER=true

# Specify versions for known vulnerabilities reported by trivy scan
# RUN apk --no-cache --update add bash libc6-compat shadow tini 'libbz2>=1.0.6-r7' 'sqlite-libs>=3.28.0-r3' && \

# Add Tini
# - https://github.com/krallin/tini
ENV TINI_VERSION v0.18.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /usr/local/bin/tini
RUN chmod +x /usr/local/bin/tini

RUN apk add --no-cache openjdk8 openjdk8-jre-lib unzip vim

# Install arthas(https://github.com/alibaba/arthas) for analyzing performance bottleneck
RUN wget -qO /tmp/arthas.zip "http://maven.aliyun.com/repository/public/com/taobao/arthas/arthas-packaging/3.4.6/arthas-packaging-3.4.6-bin.zip" && \
  mkdir -p /opt/arthas && \
  unzip /tmp/arthas.zip -d /opt/arthas && \
  rm /tmp/arthas.zip

# Install async-profiler(https://github.com/jvm-profiling-tools/async-profiler/releases/tag/v1.8.3)
RUN wget -qO /tmp/async-profiler-1.8.3-linux-x64.tar.gz "https://github.com/jvm-profiling-tools/async-profiler/releases/download/v1.8.3/async-profiler-1.8.3-linux-x64.tar.gz" && \
  tar -xvf /tmp/async-profiler-1.8.3-linux-x64.tar.gz -C /opt && \
  mv /opt/async-profiler-* /opt/async-profiler && \
  rm /tmp/async-profiler-1.8.3-linux-x64.tar.gz

ENV JAVA_HOME /usr/lib/jvm/java-1.8-openjdk

# disable JVM DNS cache
RUN echo "networkaddress.cache.ttl=0" >> ${JAVA_HOME}/jre/lib/security/java.security

# add the following for native libraries needed by rocksdb
ENV LD_LIBRARY_PATH /lib64:${LD_LIBRARY_PATH}

# if Alluxio user, group, gid, and uid aren't root|0
# then create the alluxio user and set file permissions accordingly
RUN if [ ${ALLUXIO_USERNAME} != "root" ] \
    && [ ${ALLUXIO_GROUP} != "root" ] \
    && [ ${ALLUXIO_UID} -ne 0 ] \
    && [ ${ALLUXIO_GID} -ne 0 ]; then \
      addgroup --gid ${ALLUXIO_GID} ${ALLUXIO_GROUP} && \
      adduser --system --uid ${ALLUXIO_UID} -G ${ALLUXIO_GROUP} ${ALLUXIO_USERNAME} && \
      echo "${ALLUXIO_USERNAME} ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers && \
      mkdir -p /journal && \
      chown -R ${ALLUXIO_UID}:${ALLUXIO_GID} /journal && \
      chmod -R g=u /journal && \
      mkdir /mnt/alluxio-fuse && \
      chown -R ${ALLUXIO_UID}:${ALLUXIO_GID} /mnt/alluxio-fuse; \
    fi

# Docker 19.03+ required to expand variables in --chown argument
# https://github.com/moby/buildkit/pull/926#issuecomment-503943557
COPY --chown=${ALLUXIO_USERNAME}:${ALLUXIO_GROUP} conf /opt/alluxio/conf/
COPY --chown=${ALLUXIO_USERNAME}:${ALLUXIO_GROUP} entrypoint.sh /

RUN if [ ${ENABLE_DYNAMIC_USER} = "true" ] ; then \
       chmod -R 777 /journal; \
       chmod -R 777 /mnt; \
       # Enable user_allow_other option for fuse in non-root mode
       echo "user_allow_other" >> /etc/fuse.conf; \
    fi

USER ${ALLUXIO_UID}

WORKDIR /opt/alluxio

ENV PATH="/opt/alluxio/bin:${PATH}"

ENTRYPOINT ["/entrypoint.sh"]
