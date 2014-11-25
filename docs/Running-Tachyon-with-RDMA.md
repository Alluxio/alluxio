---
layout: global
title: Running Tachyon with RDMA over JXIO
---

## Prerequisite

* **Compile JXIO**. Download and compile [JXIO](https://github.com/accelio/JXIO/).
JXIO supports Tachyon as from commit 3df475c724dd534dc4c4d79c1f375f538203446b
* **Install JXIO**.

    mvn install:install-file -Dfile=jxio.jar -DgroupId=org.accelio -DartifactId=jxio -Dversion=<version> -Dpackaging=jar


## Configure

### Worker Configuration

Configure worker to use RDMA server

<table class="table">
<tr><th>Property Name</th><th>value</th></tr>
<tr>
  <td>tachyon.worker.data.server.class</td>
  <td>tachyon.worker.rdma.RDMADataServer</td>
</tr>
</table>

### User Configuration

Configure user to use RDMA client

<table class="table">
<tr><th>Property Name</th><th>value</th></tr>
<tr>
  <td>tachyon.user.remote.block.reader.class</td>
  <td>tachyon.client.rdma.RDMARemoteBlockReader</td>
</tr>
</table>


## Compile Tachyon

    mvn install -Pjxio

