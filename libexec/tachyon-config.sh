#!/usr/bin/env bash

# Included in all the Tachyon scripts with source command should not be executable directly also
# should not be passed any arguments, since we need original $*

# resolve links - $0 may be a softlink
this="${BASH_SOURCE-$0}"
common_bin=$(cd -P -- "$(dirname -- "$this")" && pwd -P)
script="$(basename -- "$this")"
this="$common_bin/$script"

# convert relative path to absolute path
config_bin=`dirname "$this"`
script=`basename "$this"`
config_bin=`cd "$config_bin"; pwd`
this="$config_bin/$script"

# Allow for a script which overrides the default settings for system integration folks.
[ -f "$common_bin/tachyon-layout.sh" ] && . "$common_bin/tachyon-layout.sh"

# This will set the default installation for a tarball installation while os distributors can create
# their own tachyon-layout.sh file to set system installation locations.
if [ -z "$TACHYON_SYSTEM_INSTALLATION" ]; then
  VERSION=0.9.0-RC1
  export TACHYON_PREFIX=`dirname $(dirname "$this")`
  export TACHYON_HOME=${TACHYON_PREFIX}
  export TACHYON_CONF_DIR="$TACHYON_HOME/conf"
  export TACHYON_LOGS_DIR="$TACHYON_HOME/logs"
  export TACHYON_JARS="$TACHYON_HOME/assembly/target/tachyon-assemblies-${VERSION}-jar-with-dependencies.jar"
fi

# Make sure tachyon-env.sh exists
if [ ! -e $TACHYON_CONF_DIR/tachyon-env.sh ]; then
  echo "Cannot find tachyon-env.sh in $TACHYON_CONF_DIR."
  echo "Please create one manually or using '$TACHYON_HOME/bin/tachyon bootstrap-conf'."
  exit 1
fi

. $TACHYON_CONF_DIR/tachyon-env.sh

# A developer option to prepend Tachyon jars before TACHYON_CLASSPATH jars
if [ -n "$TACHYON_PREPEND_TACHYON_CLASSES" ]; then
  export CLASSPATH="$TACHYON_CONF_DIR/:$TACHYON_JARS:$TACHYON_CLASSPATH"
else
  export CLASSPATH="$TACHYON_CONF_DIR/:$TACHYON_CLASSPATH:$TACHYON_JARS"
fi
