FROM openjdk:7-jdk-alpine
MAINTAINER Andrew Audibert (aaudibert10@gmail.com)

ARG ALLUXIO_TARBALL=http://downloads.alluxio.org/downloads/files/1.4.0/alluxio-1.4.0-bin.tar.gz

RUN apk add --update bash && \
    rm -rf /var/cache/apk/*

ADD ${ALLUXIO_TARBALL} /opt/

# if the tarball was remote, it needs to be untarred
RUN cd /opt && \
    (tar -xzf alluxio* || true) && \
    (rm *.tar.gz || true)

COPY entrypoint.sh /

ENTRYPOINT ["/entrypoint.sh"]

