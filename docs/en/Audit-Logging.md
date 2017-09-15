---
layout: global
title: Audit Logging
group: Features
priority: 99
---

* Table of Contents
{:toc}

## Overview
Alluxio supports audit logging of user access to your data. With Audit logging, system administrators can
keep track of each user's access to data in Alluxio.

## Enable Audit Logging
To enable Alluxio audit logging, you need to set the JVM property. In ./conf/alluxio-env.sh, add the following two lines:

```bash
ALLUXIO_MASTER_JAVA_OPTS+=" -Dalluxio.master.audit.logging.enabled=true"
ALLUXIO_MASTER_JAVA_OPTS+=" -Dalluxio.master.audit.logger.type=${ALLUXIO_MASTER_AUDIT_LOGGER:-MASTER_AUDIT_LOGGER}"
```
Then you need to restart Alluxio, and you will find a file named `master_audit.log` in Alluxio's logs directory. The format
of the file conforms to HDFS's audit log file.
