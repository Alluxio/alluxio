---
layout: global
title: Configuration-Settings
---

Tachyon configuration parameters fall into four categories:
Master/Worker/Common(Master and Worker)/User configurations. The
environment configuration file responsible for setting system properties
is under conf/tachyon-env.sh. A template is provided with the zip:
conf/tachyon-env.sh.template.

[](#wiki-common-configuration)Common Configuration
--------------------------------------------------

The common configuration contains constants which specify paths and the
log appender name.

  Property Name             Default                                     Represents
  ------------------------- ------------------------------------------- ---------------------------------------------------------------
  tachyon.home              Tachyon installation folder                 The path to the Tachyon directory.
  tachyon.underfs.address   "tachyon.home"                              The path to the underlayer file system.
  tachyon.data.folder       "tachyon.underfs.address"/tachyon/data      The path to the data folder in the underlayer file system.
  tachyon.workers.folder    "tachyon.underfs.address"/tachyon/workers   The path to the workers folder in the underlayer file system.

[](#wiki-master-configuration)Master Configuration
--------------------------------------------------

The master configuration specifies information regarding the master
node, such as address and port number.

  Property Name                   Default                   Represents
  ------------------------------- ------------------------- -------------------------------------------------------------------------------------------------------------------------------------------------------------------
  tachyon.master.journal.folder   "tachyon.home"/journal/   The folder to store master journal log
  tachyon.master.hostname         localhost                 The externally visible hostname of Tachyon's master address
  tachyon.master.port             19998                     The port Tachyon's master node runs on.
  tachyon.master.web.port         19999                     The port Tachyon's web interface runs on.
  tachyon.master.whitelist        /                         The list of prefixes of the paths which are cacheable, separated by semi-colons. Tachyon will try to cache the cacheable file when it is read for the first time.
  tachyon.master.pinlist                                    The list of files that will remain in memory all the time. If the memory size is not sufficient, the exception will be raised for the last caching file's client.

[](#wiki-worker-configuration)Worker Configuration
--------------------------------------------------

The worker configuration specifies information regarding the worker
nodes, such as address and port number.

  Property Name                Default        Represents
  ---------------------------- -------------- -------------------------------------------------------------------------------------------------------
  tachyon.worker.port          29998          The port Tachyon's worker node runs on.
  tachyon.worker.data.port     29999          The port Tachyon's worker's data server runs on.
  tachyon.worker.data.folder   /mnt/ramdisk   The path to the data folder for Tachyon's worker nodes. Note for macs the value should be "/Volumes/"
  tachyon.worker.memory.size   128 MB         Memory capacity of each worker node.

[](#wiki-user-configuration)User Configuration
----------------------------------------------

The user configuration specifies values regarding file system access.

  Property Name                              Default   Represents
  ------------------------------------------ --------- ----------------------------------------------------------------------------------------
  tachyon.user.failed.space.request.limits   3         The number of times to request space from the file system before aborting
  tachyon.user.quota.unit.bytes              8 MB      The minimum number of bytes that will be requested from a client to a worker at a time
  tachyon.user.file.buffer.bytes             1 MB      The size of the file buffer to use for file system reads/writes.

[](#wiki-example-tachyon-envsh-file)Example tachyon-env.sh File
---------------------------------------------------------------

    # Worker size set to 512 MB 
    # Set worker folder to /Volumes/ramdist/tachyonworker

    export TACHYON_JAVA_OPTS="
      -Dtachyon.worker.memory.size=512MB 
      -Dtachyon.worker.data.folder=/Volumes/ramdisk/tachyonworker/
    "

