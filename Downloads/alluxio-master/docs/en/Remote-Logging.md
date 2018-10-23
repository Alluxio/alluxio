---
layout: global
title: Remote Logging
group: Features
priority: 5
---

* Table of Contents
{:toc}

## Overview
Alluxio supports sending logs to a remote log server over the network. This feature can be useful
to system administrators who have to perform the task of log collection. With remote logging, the
log files, e.g. master.log, worker.log, etc. on all Alluxio servers will be readily available on
a designated and configurable directory on the log server.

## Configure Alluxio
Refer to [Running Alluxio on a cluster](Running-Alluxio-on-a-Cluster.html) for instructions in deploying
Alluxio in a cluster.

By default, remote logging is not enabled. To enable Alluxio remote logging, you can set
three environment variables: `ALLUXIO_LOGSERVER_HOSTNAME`, `ALLUXIO_LOGSERVER_PORT` and `ALLUXIO_LOGSERVER_LOGS_DIR`.

There is no requirement on where
the log server must run, as long as the other Alluxio servers have access to it. In our example, we
run the log server on the same machine as a master.

### Enable Remote Logging with Environment Variables
Suppose the hostname of the log server is `AlluxioLogServer`, and the port is `45010`.
In `conf/alluxio-env.sh`, add the following lines:

```bash
ALLUXIO_LOGSERVER_HOSTNAME=AlluxioLogServer
ALLUXIO_LOGSERVER_PORT=45010
ALLUXIO_LOGSERVER_LOGS_DIR=/tmp/alluxio_remote_logs
```

## Restart Alluxio And Log Server
After making the modification to configuration, you need to restart the log server first. Then you can
start Alluxio. This ensures that the logs that Alluxio generates during start-up phase will also go to
the log server.

### Start Log Server
On the log server, execute the following command.
```bash
$ ./bin/alluxio-start.sh logserver
```

### Start Alluxio
On Alluxio master, execute the following command.
```bash
$ ./bin/alluxio-start.sh all SudoMount
```

## Verify Log Server Has Started
First, ssh to the machine on which log server is running.

Second, go to the directory where the log server has been configured to store logs received from
other Alluxio servers. In the above example, the directory is `/tmp/alluxio_remote_logs`.

```bash
$ cd /tmp/alluxio_remote_logs
$ ls
master          proxy           secondary_master    worker
$ ls -l master/
...
-rw-r--r--  1 alluxio  alluxio  26109 Sep 13 08:49 34.204.198.64.log
...
```

You can see that the log files are put into different folders according to their type. Master logs are put
in the folder `master`, worker logs are put in folder `worker`, etc. Within each folder, log files from
different workers are distinguished by the IP/hostname of the machine on which the server has been running.
