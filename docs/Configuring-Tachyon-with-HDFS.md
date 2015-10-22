---
layout: global
title: Configuring Tachyon with HDFS
nickname: Tachyon with HDFS
group: Under Stores
priority: 3
---

This guide describes how to configure Tachyon with
[HDFS](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsUserGuide.html)
as the under storage system.

# Initial Setup

To run a Tachyon cluster on a set of machines, you must deploy Tachyon binaries to each of these
machines. You can either
[compile the binaries from Tachyon source code](Building-Tachyon-Master-Branch.html), or
[download the precompiled binaries directly](Running-Tachyon-Locally.html).

Note that, by default, Tachyon binaries are built to work with Hadoop HDFS version `2.2.0`. To use
another Hadoop version, one needs to recompile Tachyon binaries from source code with the correct
Hadoop version set by either of following approaches. Assume `${TACHYON_HOME}` is the root directory
of Tachyon source code.

* Modify the `hadoop.version` tag defined in `${TACHYON_HOME}/pom.xml`. E.g., to work with Hadoop
`2.6.0`, modify this pom file to set "`<hadoop.version>2.6.0</hadoop.version>`" instead of
"`<hadoop.version>2.2.0</hadoop.version>`". Then recompile the source using maven.

```bash
$ mvn clean package
```

* Alternatively, you can also pass the correct Hadoop version in command line when compiling with
maven. For example, if you want Tachyon to work with Hadoop HDFS `2.6.0`:

```bash
$ mvn -Dhadoop.version=2.6.0 clean package
```

If everything succeeds, you should see
`tachyon-assemblies-{{site.TACHYON_RELEASED_VERSION}}-jar-with-dependencies.jar` created in the
`assembly/target` directory and this is the jar file you can use to run both Tachyon Master and Worker.

# Configuring Tachyon

To run Tachyon binary, we must setup configuration files. Create your configuration file from the
template:

```bash
$ cp conf/tachyon-env.sh.template conf/tachyon-env.sh
```

Then edit `tachyon-env.sh` file to set the under storage address to the HDFS namenode address
(e.g., `hdfs://localhost:9000` if you are running the HDFS namenode locally with default port).

```bash
export TACHYON_UNDERFS_ADDRESS=hdfs://NAMENODE:PORT
```

# Running Tachyon Locally with HDFS

After everything is configured, you can start up Tachyon locally to see that everything works.

```bash
$ ./bin/tachyon format
$ ./bin/tachyon-start.sh local
```

This should start one Tachyon master and one Tachyon worker locally. You can see the master UI at
[http://localhost:19999](http://localhost:19999).

Next, you can run a simple example program:

```bash
$ ./bin/tachyon runTest Basic STORE SYNC_PERSIST
```

For the first sample program, you should be able to see something similar to the following:

```bash
/default_tests_files/BasicFile_STORE_SYNC_PERSIST has been removed
2015-10-21 15:43:44,727 INFO   (ClientBase.java:connect) - Tachyon client (version 0.8.1-SNAPSHOT) is trying to connect with FileSystemMaster master @ localhost/127.0.0.1:19998
2015-10-21 15:43:44,740 INFO   (ClientBase.java:connect) - Client registered with FileSystemMaster master @ localhost/127.0.0.1:19998
2015-10-21 15:43:44,761 INFO   (BasicOperations.java:createFile) - createFile with fileId 83886079 took 41 ms.
2015-10-21 15:43:45,103 WARN  NativeCodeLoader (NativeCodeLoader.java:<clinit>) - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
2015-10-21 15:43:45,721 INFO   (ClientBase.java:connect) - Tachyon client (version 0.8.1-SNAPSHOT) is trying to connect with BlockMaster master @ localhost/127.0.0.1:19998
2015-10-21 15:43:45,721 INFO   (ClientBase.java:connect) - Client registered with BlockMaster master @ localhost/127.0.0.1:19998
2015-10-21 15:43:45,751 INFO   (WorkerClient.java:connect) - Connecting local worker @ /192.168.1.5:29998
2015-10-21 15:43:45,781 INFO   (FileUtils.java:createStorageDirPath) - Folder /Volumes/ramdisk/tachyonworker/8105207419420474421 was created!
2015-10-21 15:43:45,783 INFO   (LocalBlockOutStream.java:<init>) - LocalBlockOutStream created new file block, block path: /Volumes/ramdisk/tachyonworker/8105207419420474421/67108864
2015-10-21 15:43:46,411 INFO   (WorkerClient.java:connect) - Connecting local worker @ /192.168.1.5:29998
2015-10-21 15:43:47,147 INFO   (BasicOperations.java:writeFile) - writeFile to file /default_tests_files/BasicFile_STORE_SYNC_PERSIST took 2385 ms.
2015-10-21 15:43:47,187 INFO   (BasicOperations.java:readFile) - readFile file /default_tests_files/BasicFile_STORE_SYNC_PERSIST took 40 ms.
Passed the test!
```

And you can visit Tachyon web UI at **[http://localhost:19999](http://localhost:19999)** again.
Click `Browse File System` in the navigation bar and you should see the file
`/default_tests_files/BasicFile_STORE_SYNC_PERSIST` written to Tachyon by
the above test. You can also visit HDFS web UI at 
**[http://localhost:50070](http://localhost:50070)**
to verify the files and directories created by Tachyon exist in HDFS. 
For this test, you should see a file with the same name and path 
`/default_tests_files/BasicFile_STORE_SYNC_PERSIST`.

To run a more comprehensive sanity check:

```bash
$ ./bin/tachyon runTests
```

You can stop Tachyon any time by running:

```bash
$ ./bin/tachyon-stop.sh
```
