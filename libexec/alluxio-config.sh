#!/usr/bin/env bash

# Included in all the Alluxio scripts with source command should not be executable directly also
# should not be passed any arguments, since we need original $*

# resolve links - $0 may be a softlink
this="${BASH_SOURCE-$0}"
common_bin=$(cd -P -- "$(dirname -- "$this")" && pwd -P)
script="$(basename -- "$this")"
this="$common_bin/$script"

# convert relative path to absolute path
config_bin=$(dirname "$this")
script=$(basename "$this")
config_bin=$(cd "$config_bin"; pwd)
this="$config_bin/$script"

# Allow for a script which overrides the default settings for system integration folks.
[ -f "$common_bin/alluxio-layout.sh" ] && . "$common_bin/alluxio-layout.sh"

# This will set the default installation for a tarball installation while os distributors can create
# their own alluxio-layout.sh file to set system installation locations.
if [[ -z "$ALLUXIO_SYSTEM_INSTALLATION" ]]; then
  VERSION=1.1.0-SNAPSHOT
  export ALLUXIO_PREFIX=$(dirname $(dirname "$this"))
  export ALLUXIO_HOME=${ALLUXIO_PREFIX}
  export ALLUXIO_CONF_DIR="$ALLUXIO_HOME/conf"
  export ALLUXIO_LOGS_DIR="$ALLUXIO_HOME/logs"
  export ALLUXIO_JARS="$ALLUXIO_HOME/assembly/target/alluxio-assemblies-${VERSION}-jar-with-dependencies.jar"
fi

# Make sure alluxio-env.sh exists
if [[ ! -e $ALLUXIO_CONF_DIR/alluxio-env.sh ]]; then
  echo "Cannot find alluxio-env.sh in $ALLUXIO_CONF_DIR."
  echo "Please create one manually or using '$ALLUXIO_HOME/bin/alluxio bootstrap-conf'."
  exit 1
fi

. $ALLUXIO_CONF_DIR/alluxio-env.sh

# A developer option to prepend Alluxio jars before ALLUXIO_CLASSPATH jars
if [[ -n "$ALLUXIO_PREPEND_ALLUXIO_CLASSES" ]]; then
  export CLASSPATH="$ALLUXIO_CONF_DIR/:$ALLUXIO_JARS:$ALLUXIO_CLASSPATH"
else
  export CLASSPATH="$ALLUXIO_CONF_DIR/:$ALLUXIO_CLASSPATH:$ALLUXIO_JARS"
fi
