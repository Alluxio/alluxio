from ubuntu:18.04

ARG ALLUXIO_TARBALL
ARG VERSION
ARG PKG_REVISION=0
ARG PKG_NAME=alluxio_${VERSION}-${PKG_REVISION}
ARG PKG_DIR=/${PKG_NAME}

RUN apt-get update && apt-get install -y \
    debhelper

COPY ${ALLUXIO_TARBALL} ${PKG_DIR}/opt/${ALLUXIO_TARBALL}
COPY control ${PKG_DIR}/DEBIAN/control
COPY control ${PKG_DIR}/debian/control

RUN cd ${PKG_DIR}/opt && tar -xzf *.tar.gz && rm *.tar.gz

# Create links
RUN cd ${PKG_DIR} && dh_link ${PKG_DIR}/opt/alluxio-* ${PKG_DIR}/opt/alluxio

RUN mkdir -p ${PKG_DIR}/etc/profile.d/ && \
    echo "export PATH=\"${PATH}:/opt/${PKG_NAME}/bin\"" >> ${PKG_DIR}/etc/profile.d/10-${PKG_NAME}.sh
RUN ls -l ${PKG_DIR}/debian
RUN ls -l ${PKG_DIR}/DEBIAN
RUN dpkg-deb --build ${PKG_NAME}