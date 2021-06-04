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

FROM golang:1.15.13-alpine as dev

ENV GO111MODULE=on

RUN mkdir -p $GOPATH/src/github.com/Alluxio/alluxio-csi
COPY . $GOPATH/src/github.com/Alluxio/alluxio-csi

FROM dev as build

RUN cd $GOPATH/src/github.com/Alluxio/alluxio-csi && \
    CGO_ENABLED=0 go build -o /usr/local/bin/alluxio-csi

FROM alluxio/alluxio-fuse:latest as final
COPY --from=build /usr/local/bin/alluxio-csi /usr/local/bin/
