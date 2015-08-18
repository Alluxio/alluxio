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

vagrant up
vagrant package --output tachyon-dev.box default
vagrant destroy -f
vagrant box add tachyon-dev tachyon-dev.box

popd >/dev/null
