---
layout: global
title: Audit Logging
group: Features
priority: 1
---

* Table of Contents
{:toc}

## Enable Audit Logging
To enable Alluxio audit logging, you need to set the JVM property `alluxio.master.audit.logging.enabled` to true. Besides,
you also need to configure the proper log4j appender type for the audit log. Alluxio already provides a default log
appender called `MASTER_AUDIT_LOGGER`. This is sufficient for most users. Advanced users can use their own appender and set
the environment variable `${ALLUXIO_MASTER_AUDIT_LOGGER}` to the name of their custom appender.
In `./conf/alluxio-env.sh`, add the following two lines:

```bash
ALLUXIO_MASTER_JAVA_OPTS+=" -Dalluxio.master.audit.logging.enabled=true"
ALLUXIO_MASTER_JAVA_OPTS+=" -Dalluxio.master.audit.logger.type=${ALLUXIO_MASTER_AUDIT_LOGGER:-MASTER_AUDIT_LOGGER}"
```
Then you need to restart Alluxio, and you will find a file named `master_audit.log` in Alluxio's logs directory.
