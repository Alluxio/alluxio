# Building tachyon-dev.box 

This module is to build a virtualbox base vm with basic tachyon development environment including java, maven, git, rsync, wget, libselinux-python etc.. The provisioning tasks are defined by the ansible playbook `../provision/playbook-basic.yml`.  

`build_box.sh` is the scripts that build the box named tachyon-dev, and register it to vagrant boxes. 
The generated box will be in the same directory as this file, named "tachyon-dev.box".

When `create` script in the parent directory is called with `vb` as the vm provider for the first time, the `build_box.sh` in this module will be run first to prepare the base box. Users can also run `build_box.sh` to build tachyon-dev.box directly.
