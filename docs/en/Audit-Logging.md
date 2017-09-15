---
layout: global
title: Audit Logging
group: Features
priority: 1
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
of the file is very similar to HDFS's audit log file.

<table class="table table-striped">
<tr><th>key</th><th>value</th></tr>
<tr>
  <td>succeeded</td>
  <td>True if the command has succeeded. To succeed, it must also have been allowed. </td>
</tr>
<tr>
  <td>allowed</td>
  <td>True if the command has been allowed. Note that a command can still fail even if it has been allowed. </td>
</tr>
<tr>
  <td>ugi</td>
  <td>User, primary group, authentication type. </td>
</tr>
<tr>
  <td>ip</td>
  <td>Client ip address. </td>
</tr>
<tr>
  <td>cmd</td>
  <td>Command issued by the user. </td>
</tr>
<tr>
  <td>src</td>
  <td>Path of the source file or directory. </td>
</tr>
<tr>
  <td>dst</td>
  <td>Path of the destination file or directory. If not application, the value is null. </td>
</tr>
<tr>
  <td>perm</td>
  <td>User:group:mask or null if not applicable. </td>
</tr>
</table>
