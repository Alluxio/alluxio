#!/usr/bin/env bash

# This file is sourced when launching Alluxio daemon or using Alluxio shell
# commands. It provides one way to configure a few Alluxio configuration options
# by setting the following environment variables. Note that, settings in this
# file will not affect the jobs (e.g., Spark job or MapReduce job) that are
# using Alluxio client as a library. Alternatively, you can edit
# alluxio.properties file, where you can set all configuration options supported
# by Alluxio
# (http://alluxio.org/documentation/master/en/Configuration-Settings.html), and
# it is respected by both the jobs and Alluxio daemon (or shell).

# The directory where Alluxio deployment is installed
# ALLUXIO_HOME

# The directory where log files are stored. (Default: ${ALLUXIO_HOME}/logs).
# ALLUXIO_LOGS_DIR

# Address of the master.
# ALLUXIO_MASTER_ADDRESS

# The directory where a worker stores in-memory data. (Default: /mnt/ramdisk).
# E.g. On linux,  /mnt/ramdisk for ramdisk, /dev/shm for tmpFS; on MacOS, /Volumes/ramdisk for ramdisk
# ALLUXIO_RAM_FOLDER

# Address of the under filesystem address. (Default: ${ALLUXIO_HOME}/underFSStorage)
# E.g. "/my/local/path" to use local fs, "hdfs://localhost:9000/alluxio" to use a local hdfs
# ALLUXIO_UNDERFS_ADDRESS

# How much memory to use per worker
# E.g. "1000mb", "2gb"
# ALLUXIO_WORKER_MEMORY_SIZE

# Config properties set for Alluxio master daemon. (Default: "")
# E.g. "-Dalluxio.master.port=39999"
# ALLUXIO_MASTER_JAVA_OPTS

# Config properties set for Alluxio worker daemon. (Default: "")
# E.g. "-Dalluxio.worker.port=49999"
# ALLUXIO_WORKER_JAVA_OPTS

# Config properties set for Alluxio shell. (Default: "")
# E.g. "-Dalluxio.user.file.writetype.default=THROUGH"
# ALLUXIO_USER_JAVA_OPTS
