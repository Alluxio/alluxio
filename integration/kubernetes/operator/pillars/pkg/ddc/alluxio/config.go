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

enablePillars: true
`

const permissions = 0755
