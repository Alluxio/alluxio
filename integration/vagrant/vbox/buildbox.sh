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


OLD_BOX="$(vagrant box list | grep alluxio-dev | cut -d ' ' -f1)"
if [[ "${OLD_BOX}" != "" ]]; then
  echo "Alluxio base image ${OLD_BOX} exists."
  echo "If you want to remove image ${OLD_BOX}, please run: vagrant box remove ${OLD_BOX}"
  exit 0
fi

HERE="$(dirname $0)"
pushd "${HERE}" > /dev/null

if [[ -f alluxio-dev.box ]]; then
  rm -f alluxio-dev.box
fi

echo "Generating alluxio base image 'alluxio-dev.box' ..."
vagrant up
vagrant package --output alluxio-dev.box default
vagrant destroy -f
vagrant box add alluxio-dev alluxio-dev.box

popd > /dev/null
