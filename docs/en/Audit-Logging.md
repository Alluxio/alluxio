---
layout: global
title: Audit Logging
group: Features
priority: 99
---

* Table of Contents
{:toc}

## Overview
Alluxio supports auditing of user access to your data.

## Enable Audit Logging
To enable Alluxio audit logging, you need to set the JVM property. In ./conf/alluxio-env.sh, add the following lines:

```bash
ALLUXIO_MASTER_JAVA_OPTS+=" -Dalluxio.master.audit.logging.enabled=true"
ALLUXIO_MASTER_JAVA_OPTS+=" -Dalluxio.master.audit.logger.type=${ALLUXIO_MASTER_AUDIT_LOGGER:-MASTER_AUDIT_LOGGER}"
```

Then you can restart Alluxio, and you will find a file named master_audit.log in the directory where you store other logs.
