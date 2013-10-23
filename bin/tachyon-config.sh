# Included in all the Tachyon scripts with source command
# should not be executable directly
# also should not be passed any arguments, since we need original $*

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

export TACHYON_PREFIX=`dirname "$this"`/..
export TACHYON_HOME=${TACHYON_PREFIX}
export TACHYON_CONF_DIR="$TACHYON_HOME/conf"
export TACHYON_LOGS_DIR="$TACHYON_HOME/logs"
export TACHYON_JAR=$TACHYON_HOME/target/tachyon-0.4.0-SNAPSHOT-jar-with-dependencies.jar
