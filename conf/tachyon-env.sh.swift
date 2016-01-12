#!/usr/bin/env bash

# This file contains environment variables required to run Tachyon. Copy it as tachyon-env.sh and
# edit that to configure Tachyon for your site. At a minimum,
# the following variables should be set:
#
# - JAVA_HOME, to point to your JAVA installation
# - TACHYON_MASTER_ADDRESS, to bind the master to a different IP address or hostname
# - TACHYON_UNDERFS_ADDRESS, to set the under filesystem address.
# - TACHYON_WORKER_MEMORY_SIZE, to set how much memory to use (e.g. 1000mb, 2gb) per worker
# - TACHYON_RAM_FOLDER, to set where worker stores in memory data


# Support for Multihomed Networks.
# You can specify a hostname to bind each of services. If a wildcard
# is applied, you should select one of network interfaces and use its hostname to connect the service.
# If no hostname is defined, Tachyon will automatically select an externally visible localhost name.
# The various possibilities shown in the following table:
#
# +--------------------+------------------------+---------------------+
# | TACHYON_*_HOSTNAME |  TACHYON_*_BIND_HOST   | Actual Connect Host |
# +--------------------+------------------------+---------------------+
# | hostname           | hostname               | hostname            |
# | not defined        | hostname               | hostname            |
# | hostname           | 0.0.0.0 or not defined | hostname            |
# | not defined        | 0.0.0.0 or not defined | localhost           |
# +--------------------+------------------------+---------------------+
#
# Configuration Examples:
#
# Environment variables for service bind
# TACHYON_MASTER_BIND_HOST=${TACHYON_MASTER_BIND_HOST:-$(hostname -A | cut -d" " -f1)}
# TACHYON_MASTER_WEB_BIND_HOST=${TACHYON_MASTER_WEB_BIND_HOST:-0.0.0.0}
# TACHYON_WORKER_BIND_HOST=${TACHYON_WORKER_BIND_HOST:-$(hostname -A | cut -d" " -f1)}
# TACHYON_WORKER_DATA_BIND_HOST=${TACHYON_WORKER_DATA_BIND_HOST:-$(hostname -A | cut -d" " -f1)}
# TACHYON_WORKER_WEB_BIND_HOST=${TACHYON_WORKER_WEB_BIND_HOST:-0.0.0.0}
#
# Environment variables for service connection
# TACHYON_MASTER_HOSTNAME=${TACHYON_MASTER_HOSTNAME:-$(hostname -A | cut -d" " -f1)}
# TACHYON_MASTER_WEB_HOSTNAME=${TACHYON_MASTER_WEB_HOSTNAME:-$(hostname -A | cut -d" " -f1)}
# TACHYON_WORKER_HOSTNAME=${TACHYON_WORKER_HOSTNAME:-$(hostname -A | cut -d" " -f1)}
# TACHYON_WORKER_DATA_HOSTNAME=${TACHYON_WORKER_DATA_HOSTNAME:-$(hostname -A | cut -d" " -f1)}
# TACHYON_WORKER_WEB_HOSTNAME=${TACHYON_WORKER_WEB_HOSTNAME:-$(hostname -A | cut -d" " -f1)}

# The following gives an example:

# Uncomment this section to add a local installation of Hadoop to Tachyon's CLASSPATH.
# The hadoop command must be in the path to automatically populate the Hadoop classpath.
#
# if type "hadoop" > /dev/null 2>&1; then
#  export HADOOP_TACHYON_CLASSPATH=$(hadoop classpath)
# fi
# export TACHYON_CLASSPATH=${TACHYON_CLASSPATH:-${HADOOP_TACHYON_CLASSPATH}}

if [[ $(uname -s) == Darwin ]]; then
  # Assuming Mac OS X
  export JAVA_HOME=${JAVA_HOME:-$(/usr/libexec/java_home)}
  export TACHYON_RAM_FOLDER=/Volumes/ramdisk
  export TACHYON_JAVA_OPTS="-Djava.security.krb5.realm= -Djava.security.krb5.kdc="
else
  # Assuming Linux
  if [[ -z "${JAVA_HOME}" ]]; then
    if [ -d /usr/lib/jvm/java-7-oracle ]; then
      export JAVA_HOME=/usr/lib/jvm/java-7-oracle
    else
      # openjdk will set this
      if [[ -d /usr/lib/jvm/jre-1.7.0 ]]; then
        export JAVA_HOME=/usr/lib/jvm/jre-1.7.0
      fi
    fi
  fi
  export TACHYON_RAM_FOLDER="/mnt/ramdisk"
fi

if [[ -z "${JAVA_HOME}" ]]; then
  export JAVA_HOME="$(dirname $(which java))/.."
fi

export JAVA="${JAVA_HOME}/bin/java"
export TACHYON_MASTER_ADDRESS=${TACHYON_MASTER_ADDRESS:-localhost}
export TACHYON_UNDERFS_ADDRESS=${TACHYON_UNDERFS_ADDRESS:-swift://tachyontestcont}
export TACHYON_WORKER_MEMORY_SIZE=${TACHYON_WORKER_MEMORY_SIZE:-1GB}

export TACHYON_SSH_FOREGROUND=${TACHYON_SSH_FOREGROUND:-"yes"}
export TACHYON_WORKER_SLEEP=${TACHYON_WORKER_SLEEP:-"0.02"}

# Prepend Tachyon classes before classes specified by TACHYON_CLASSPATH
# in the Java classpath.  May be necessary if there are jar conflicts
#export TACHYON_PREPEND_TACHYON_CLASSES=${TACHYON_PREPEND_TACHYON_CLASSES:-"yes"}

# Where log files are stored. $TACHYON_HOME/logs by default.
#export TACHYON_LOGS_DIR=${TACHYON_LOGS_DIR:-${TACHYON_HOME}/logs}

CONF_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export TACHYON_JAVA_OPTS+="
  -Dlog4j.configuration=file:${CONF_DIR}/log4j.properties
  -Dtachyon.worker.tieredstore.level.max=1
  -Dtachyon.worker.tieredstore.level0.alias=MEM
  -Dtachyon.worker.tieredstore.level0.dirs.path=${TACHYON_RAM_FOLDER}
  -Dtachyon.worker.tieredstore.level0.dirs.quota=${TACHYON_WORKER_MEMORY_SIZE}
  -Dtachyon.underfs.address=${TACHYON_UNDERFS_ADDRESS}
  -Dtachyon.worker.memory.size=${TACHYON_WORKER_MEMORY_SIZE}
  -Dtachyon.master.hostname=${TACHYON_MASTER_ADDRESS}
  -Dorg.apache.jasper.compiler.disablejsr199=true
  -Djava.net.preferIPv4Stack=true
  -Dfs.swift.user=<swift-user>
  -Dfs.swift.tenant=<swift-tenant>
  -Dfs.swift.apikey=<swift-user-password>
  -Dfs.swift.auth.url=<swift-auth-url>
  -Dfs.swift.auth.port=<swift-auth-url-port>
  -Dfs.swift.use.public.url=<swift-use-public: true, false>
  -Dfs.swift.auth.method=<swift-auth-model: keystone, tempauth>
"

# Master specific parameters. Default to TACHYON_JAVA_OPTS.
export TACHYON_MASTER_JAVA_OPTS="${TACHYON_JAVA_OPTS}"

# Worker specific parameters that will be shared to all workers. Default to TACHYON_JAVA_OPTS.
export TACHYON_WORKER_JAVA_OPTS="${TACHYON_JAVA_OPTS}"
