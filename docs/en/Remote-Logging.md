---
layout: global
title: Remote Logging
group: Features
priority: 99
---

* Table of Contents
{:toc}

## Overview
Alluxio supports sending logs to a remote log server over the network. This feature can be useful
to system administrators who have to perform the task of log collection. With remote logging, the
log files, e.g. master.log, worker.log, etc. on all Alluxio servers will be readily available on
a designated and configurable directory on the log server.

## Configure Alluxio
By default, remote logging is not enabled. To enable Alluxio remote logging, you can either set a
few environment variables or modify the JVM properties directly.

This section describes these two different approaches step by step. There is no requirement on where
the log server must run, as long as the other Alluxio servers have access to it. In our example, we
run the log server on the same machine as the primary master. If you want to run the log server on
another machine, you need to copy the entire Alluxio directory to the desired machine.

Suppose the hostname of the log server is AlluxioLogServer, and the port is 45010. Alluxio is deployed
in a cluster.

### Environment Variables
In ./conf/alluxio-env.sh, add the following lines:

```bash
ALLUXIO_LOGSERVER_HOSTNAME=AlluxioLogServer
ALLUXIO_LOGSERVER_PORT=45010
ALLUXIO_LOGSERVER_LOGS_DIR=/tmp/alluxio_remote_logs
```

### JVM Properties
In ./conf/alluxio-env.sh, add the following lines:

```bash
ALLUXIO_JAVA_OPTS=" -Dalluxio.logserver.hostname=AlluxioLogServer -Dalluxio.logserver.port=45010"
ALLUXIO_MASTER_JAVA_OPTS=" -Dalluxio.remote.logger.type=REMOTE_MASTER_LOGGER"
ALLUXIO_SECONDARY_MASTER_JAVA_OPTS=" -Dalluxio.remote.logger.type=REMOTE_SECONDARY_MASTER_LOGGER"
ALLUXIO_WORKER_JAVA_OPTS=" -Dalluxio.remote.logger.type=REMOTE_WORKER_LOGGER"
ALLUXIO_PROXY_JAVA_OPTS=" -Dalluxio.remote.logger.type=REMOTE_PROXY_LOGGER"
ALLUXIO_LOGSERVER_LOGS_DIR=/tmp/alluxio_remote_logs
```

## Restart Alluxio And Log Server
After making the modification to configuration, you need to restart Alluxio and the log server.

### Start Log Server
```bash
$ ./bin/alluxio-start.sh logserver
```

### Start Alluxio on Local Machine
```bash
$ ./bin/alluxio-start.sh all
```

## Verify Log Server Has Started
First, ssh to the log server or log in locally.

Second, go to the directory where the log server has been configured to store logs received from
other Alluxio servers. In the above example, the directory is `/tmp/alluxio_remote_logs`.

```bash
$ cd /tmp/alluxio_remote_logs
$ ls
master          proxy           secondary_master    worker
$ ls -l
...
-rw-r--r--  1 alluxio  alluxio-group  26109 Sep 13 08:49 34.204.198.64.log
...
```

user "alluxio" and group "alluxio-group" are just examples.

You can see that the log files are put into different folders according to their type. Master logs are put
in the folder `master`, worker logs are put in folder `worker`, etc. Within each folder, log files from
different workers are distinguished by the IP/hostname of the machine on which the server has been running.
