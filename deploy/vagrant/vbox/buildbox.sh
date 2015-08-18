#!/bin/bash

OLD_BOX=$(vagrant box list | grep tachyon-dev | cut -d ' ' -f1)
if [[ "$OLD_BOX" != '' ]]; then
 echo "$OLD_BOX exists"
 echo "if you want to remove $OLD_BOX, use command: vagrant box remove $OLD_BOX"
 exit 0
fi

HERE=$(dirname $0)
pushd $HERE >/dev/null

if [ -f tachyon-dev.box ]; then
  rm -f tachyon-dev.box
fi

BASE_BOX='chef/centos-7.0'
AVAIL_BOX=$(vagrant box list | grep $BASE_BOX | grep '(virtualbox, 1.0.0)')

if [ -z "$AVAIL_BOX" ]; then
  vagrant box add chef/centos-7.0 --provider virtualbox
fi

echo "Generating 'tachyon-dev.box' based on box '$BASE_BOX' ..."
vagrant box repackage $BASE_BOX virtualbox 1.0
mv package.box tachyon-dev.box
vagrant box add tachyon-dev tachyon-dev.box

popd >/dev/null
