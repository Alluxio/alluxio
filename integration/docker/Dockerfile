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

# We have to do an ADD to put the tarball into extractor, then do a COPY with chown into final
# ADD then chown in two steps will double the image size
#   See - https://stackoverflow.com/questions/30085621/why-does-chown-increase-size-of-docker-image
#       - https://github.com/moby/moby/issues/5505
#       - https://github.com/moby/moby/issues/6119
# ADD with chown doesn't chown the files inside tarball
#   See - https://github.com/moby/moby/issues/35525
FROM alpine:3.10.2 AS alluxio-extractor
# Note that downloads for *-SNAPSHOT tarballs are not available.
ARG ALLUXIO_TARBALL=http://downloads.alluxio.io/downloads/files/302-SNAPSHOT/alluxio-302-SNAPSHOT-bin.tar.gz

ADD ${ALLUXIO_TARBALL} /opt/
# Remote tarball needs to be untarred. Local tarball is untarred automatically.
# Use ln -s instead of mv to avoid issues with Centos (see https://github.com/moby/moby/issues/27358)
RUN cd /opt && \
    (if ls | grep -q ".tar.gz"; then tar -xzf *.tar.gz && rm *.tar.gz; fi) && \
    ln -s alluxio-* alluxio

FROM alluxio/alluxio-base:latest AS final

COPY --chown=${ALLUXIO_UID}:${ALLUXIO_GID} --from=alluxio-extractor /opt /opt/
