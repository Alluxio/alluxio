This module is to build a base virtualbox vm with java, maven, git, rsync, wget, libselinux-python install. 

`init.sh` will build the box named tachyon-dev, and register it to vagrant boxes. 
the generated box will be in the same directory as this file, named "tachyon-dev.box"

when run `run_vb.sh` in root module for the first time, the `init.sh` in this module will be run first to prepare the base box.
