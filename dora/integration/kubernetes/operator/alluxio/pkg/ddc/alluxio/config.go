/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio

const defaultConfig string = `
image: alluxio/alluxio
imageTag: "2.1.2"

properties:
    alluxio.fuse.debug.enabled: "false"
    alluxio.user.file.writetype.default: MUST_CACHE
    alluxio.master.journal.folder: /journal
    alluxio.master.journal.type: UFS

worker:
    jvmOptions: " -Xmx2G "

master:
    jvmOptions: " -Xmx2G "

tieredstore:
  levels:
  - alias: MEM
    level: 0
    type: hostPath
    path: /dev/shm
    high: 0.99
    low: 0.8

fuse:
  image: alluxio/alluxio-fuse
  imageTag: "2.1.2"
  jvmOptions: " -Xmx4G -Xms4G "
  args:
    - fuse
    - --fuse-opts=direct_io

enablealluxio: true
`

const permissions = 0755
