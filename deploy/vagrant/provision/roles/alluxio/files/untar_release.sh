#!/usr/bin/env bash

DIST=/vagrant/shared/${ALLUXIO_DIST}
tar xzf ${DIST} -C /alluxio --strip-components 1
