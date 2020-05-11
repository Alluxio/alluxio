#!/bin/bash
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

dest="$(pwd)/resources/opt/bootstrap/"
GOPATH="$(pwd)"
cd src/alluxio.com
env GOOS=linux GOARCH=amd64 go build -o alluxio-ami-bootstrap ./main.go
chmod u+x alluxio-ami-bootstrap
mkdir -p ${dest}
mv alluxio-ami-bootstrap ${dest}
