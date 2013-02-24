#!/usr/bin/env bash

if [ "$#" == 0 ]; then
  echo "Usage: tachyon COMMAND"
  echo "where COMMAND is one of:"
  echo -e "    tfs\t run a generic filesystem user client"
  echo "Most commands print help when invoked w/o parameters."
  exit 1
elif [[ ("$#" == 1) || ("$#" == 2) ]]; then
  echo "Usage: java TFsShell"
  echo -e "\t[-ls <path>] "
  echo -e "\t[-lsr <path>]"
  echo -e "\t[mkdir <path>]"
  echo ""
  echo "The general command line syntax is:"
  echo "bin/tachyon command [commandOptions]"
  exit 1
elif [ "$#" == 3 ]; then
	bin=`cd "$( dirname "$0" )"; pwd`
	. "$bin/tachyon-config.sh"
	java -cp $TACHYON_HOME/target/tachyon-1.0-SNAPSHOT-jar-with-dependencies.jar tachyon.command.TFsShell ${@: 2:3}  #tachyon.jar change
fi