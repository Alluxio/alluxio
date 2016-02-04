#!/bin/bash

OLD_BOX="$(vagrant box list | grep alluxio-dev | cut -d ' ' -f1)"
if [[ "${OLD_BOX}" != "" ]]; then
  echo "Tachyon base image ${OLD_BOX} exists."
  echo "If you want to remove image ${OLD_BOX}, please run: vagrant box remove ${OLD_BOX}"
  exit 0
fi

HERE="$(dirname $0)"
pushd "${HERE}" >/dev/null

if [[ -f tachyon-dev.box ]]; then
  rm -f tachyon-dev.box
fi

echo "Generating alluxio base image 'alluxio-dev.box' ..."
vagrant up
vagrant package --output tachyon-dev.box default
vagrant destroy -f
vagrant box add tachyon-dev tachyon-dev.box

popd >/dev/null
