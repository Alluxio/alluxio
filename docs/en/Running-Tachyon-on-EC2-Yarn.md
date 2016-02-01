---
layout: global
title: Running Tachyon with YARN on EC2
nickname: Tachyon on EC2 with YARN
group: User Guide
priority: 5
---

Tachyon can be started and managed by Apache YARN. This guide demonstrates how to launch Tachyon
with YARN on EC2 machines using the
[Vagrant scripts](https://github.com/amplab/tachyon/tree/master/deploy/vagrant) that come with
Tachyon.

# Prerequisites

**Install Vagrant and the AWS plugins**

Download [Vagrant](https://www.vagrantup.com/downloads.html)

Install AWS Vagrant plugin:

```bash
$ vagrant plugin install vagrant-aws
$ vagrant box add dummy https://github.com/mitchellh/vagrant-aws/raw/master/dummy.box
```

**Install Tachyon**

Download Tachyon to your local machine, and unzip it:

```bash
$ wget http://tachyon-project.org/downloads/files/{{site.TACHYON_RELEASED_VERSION}}/tachyon-{{site.TACHYON_RELEASED_VERSION}}-bin.tar.gz
$ tar xvfz tachyon-{{site.TACHYON_RELEASED_VERSION}}-bin.tar.gz
```

**Install python library dependencies**

Install [python>=2.7](https://www.python.org/), not python3.

Under `deploy/vagrant` directory in your Tachyon home directory, run:

```bash
$ sudo bash bin/install.sh
```

Alternatively, you can manually install [pip](https://pip.pypa.io/en/latest/installing/), and then
in `deploy/vagrant` run:

```bash
$ sudo pip install -r pip-req.txt
```

# Launch a Cluster

To run a Tachyon cluster on EC2, first sign up for an Amazon EC2 account
on the [Amazon Web Services site](http://aws.amazon.com/).

Then create [access keys](https://aws.amazon.com/developers/access-keys/) and set shell environment
variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` by:

```bash
$ export AWS_ACCESS_KEY_ID=<your access key>
$ export AWS_SECRET_ACCESS_KEY=<your secret access key>
```

Next generate your EC2
[Key Pairs](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html) in the region 
you want to deploy to (**us-east-1** by default). Make sure to set the permissions of your private 
key file so that only you can read it:

```bash
$ chmod 400 <your key pair>.pem
```

In the configuration file `deploy/vagrant/conf/ec2.yml`, set the value of `Keypair` to your keypair
name and `Key_Path` to the path to the pem key.

By default, the Vagrant script creates a
[Security Group](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-network-security.html)
named *tachyon-vagrant-test* at
[Region(**us-east-1**) and Availability Zone(**us-east-1a**)](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html).
The security group will be set up automatically in the region with all inbound/outbound network
traffic opened. You can change the security group, region and availability zone in `ec2.yml`.

Now you can launch the Tachyon cluster with Hadoop2.4.1 as under filesystem in us-east-1a by running
the script under `deploy/vagrant`:

```bash
$ ./create <number of machines> aws
```

# Access the cluster

**Access through Web UI**

After command `./create <number of machines> aws` succeeds, you can see two green lines like below
shown at the end of the shell output:

    >>> TachyonMaster public IP is xxx, visit xxx:19999 for Tachyon web UI<<<
    >>> visit default port of the web UI of what you deployed <<<

Default port for Tachyon Web UI is **19999**.

Default port for Hadoop Web UI is **50070**.

Visit `http://{MASTER_IP}:{PORT}` in the browser to access the Web UIs.

You can also monitor the instances state through
[AWS web console](https://console.aws.amazon.com/console/home?region=us-east-1).

**Access with ssh**

The nodes set up are named to `TachyonMaster`, `TachyonWorker1`, `TachyonWorker2` and so on.

To ssh into a node, run:

```bash
$ vagrant ssh <node name>
```

For example, you can ssh into `TachyonMaster` with:

```bash
$ vagrant ssh TachyonMaster
```

All software is installed under root directory, e.g. Tachyon is installed in `/tachyon`, Hadoop is
installed in `/hadoop`.

# Configure Tachyon integration with YARN

On our EC2 machines, YARN has been installed as a part of Hadoop version 2.4.1. Notice that, by 
default Tachyon binaries 
built by vagrant script do not include this YARN integration. You should first stop the default 
Tachyon service, re-compile Tachyon with profile "yarn" specified to have the YARN client and
ApplicationMaster for Tachyon.

```bash
$ cd /tachyon
$ ./bin/tachyon-stop.sh all
$ mvn clean install -Dhadoop.version=2.4.1 -Pyarn -DskipTests -Dfindbugs.skip -Dmaven.javadoc.skip -Dcheckstyle.skip
```

Note that adding `-DskipTests -Dfindbugs.skip -Dmaven.javadoc.skip -Dcheckstyle.skip` is not strictly necessary,
but it makes the build run significantly faster.

To customize Tachyon master and worker with specific properties (e.g., tiered storage setup on each
worker), one can refer to [Configuration settings](Configuration-Settings.html) for more
information. To ensure your configuration can be read by both the ApplicationMaster and Tachyon
master/workers, put `tachyon-site.properties` under `${TACHYON_HOME}/conf` on each EC2 machine.

# Start Tachyon

Use script `integration/bin/tachyon-yarn.sh` to start Tachyon. This script requires three arguments:
1. A path pointing to `${TACHYON_HOME}` on each machine so YARN NodeManager can access Tachyon
scripts and binaries to launch masters and workers. With our EC2 setup, this path is `/tachyon`.
2. The total number of Tachyon workers to start.
3. A HDFS path to distribute the binaries for Tachyon ApplicationMaster.

For example, here we launch a Tachyon cluster with 3 worker nodes, where an HDFS temp directory is
`hdfs://TachyonMaster:9000/tmp/` and each YARN container can access Tachyon in `/tachyon`

```bash
$ export HADOOP_HOME=/hadoop
$ /hadoop/bin/hadoop fs -mkdir hdfs://TachyonMaster:9000/tmp
$ /tachyon/integration/bin/tachyon-yarn.sh /tachyon 3 hdfs://TachyonMaster:9000/tmp/
```

This script will first upload the binaries with YARN client and ApplicationMaster to the HDFS path
specified, then inform YARN to run the client binary jar. The script will keep running with
ApplicationMaster status reported. You can also check `http://TachyonMaster:8088` in the browser to
access the Web UIs and watch the status of the Tachyon job as well as the application ID.

The output of the above script may produce output like the following:

```bash
Using $HADOOP_HOME set to '/hadoop'
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/hadoop/share/hadoop/common/lib/slf4j-log4j12-1.7.5.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/tachyon/clients/client/target/tachyon-client-0.8.1-SNAPSHOT-jar-with-dependencies.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.slf4j.impl.Log4jLoggerFactory]
Initializing Client
Starting Client
15/10/22 00:01:17 INFO client.RMProxy: Connecting to ResourceManager at TachyonMaster/172.31.22.124:8050
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/hadoop/share/hadoop/common/lib/slf4j-log4j12-1.7.5.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/tachyon/clients/client/target/tachyon-client-0.8.1-SNAPSHOT-jar-with-dependencies.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.slf4j.impl.Log4jLoggerFactory]
ApplicationMaster command: {{JAVA_HOME}}/bin/java -Xmx256M tachyon.yarn.ApplicationMaster 3 /tachyon localhost 1><LOG_DIR>/stdout 2><LOG_DIR>/stderr 
Submitting application of id application_1445469376652_0002 to ResourceManager
15/10/22 00:01:19 INFO impl.YarnClientImpl: Submitted application application_1445469376652_0002
2015/10/22 00:01:29 Got application report from ASM for appId=2, clientToAMToken=null, appDiagnostics=, appMasterHost=, appQueue=default, appMasterRpcPort=0, appStartTime=1445472079196, yarnAppState=RUNNING, distributedFinalState=UNDEFINED, appTrackingUrl=http://TachyonMaster:8088/proxy/application_1445469376652_0002/A, appUser=ec2-user
2015/10/22 00:01:39 Got application report from ASM for appId=2, clientToAMToken=null, appDiagnostics=, appMasterHost=, appQueue=default, appMasterRpcPort=0, appStartTime=1445472079196, yarnAppState=RUNNING, distributedFinalState=UNDEFINED, appTrackingUrl=http://TachyonMaster:8088/proxy/application_1445469376652_0002/A, appUser=ec2-user
```

From the output, we know the application ID to run Tachyon is 
**`application_1445469376652_0002`**. This application ID is needed to kill the application.

NOTE: currently Tachyon YARN framework does not guarantee to start the Tachyon master on the
TachyonMaster machine; use the YARN Web UI to read the logs of this YARN application. The log
of this application records which machine is used to launch a Tachyon master container like:

```
15/10/22 21:50:23 INFO : Launching container container_1445550205172_0001_01_000002 for Tachyon master on TachyonMaster:8042 with master command: [/tachyon/integration/bin/tachyon-master-yarn.sh 1><LOG_DIR>/stdout 2><LOG_DIR>/stderr ]
```

# Test Tachyon

When you know the IP of Tachyon master container, you can modify the `conf/tachyon-env.sh` to set
 up environment variable `TACHYON_MASTER_ADDRESS` on each EC2 machine:

```
export TACHYON_MASTER_ADDRESS=TACHYON_MASTER_IP
```

You can run tests against Tachyon to check its health:

```bash
$ /tachyon/bin/tachyon runTests
```

After the tests finish, visit Tachyon web UI at `http://TACHYON_MASTER_IP:19999` again. Click
`Browse File System` in the navigation bar, and you should see the files written to Tachyon by the above
tests.


# Stop Tachyon

Tachyon can be stopped by using the following YARN command where the application ID of Tachyon can
be retrieved from either YARN web UI or the output of `tachyon-yarn.sh` as mentioned above. For
instance, if the application Id is `application_1445469376652_0002`, you can stop Tachyon by killing
the application using:

```bash
$ /hadoop/bin/yarn application -kill application_1445469376652_0002
```

# Destroy the cluster

Under `deploy/vagrant` directory in your local machine where EC2 machines are launched, you can run:

```bash
$ ./destroy
```

to destroy the cluster that you created. Only one cluster can be created at a time. After the
command succeeds, the EC2 instances are terminated.

# Trouble Shooting

1 If you compile Tachyon with YARN integration using maven and see compilation errors like the
following messages:

 ```
 [ERROR] Failed to execute goal org.apache.maven.plugins:maven-compiler-plugin:3.2:compile (default-compile) on project tachyon-integration-yarn: Compilation failure: Compilation failure:
 [ERROR] /tachyon/upstream/integration/yarn/src/main/java/tachyon/yarn/Client.java:[273,49] cannot find symbol
 [ERROR] symbol:   method $$()
 [ERROR] location: variable JAVA_HOME of type org.apache.hadoop.yarn.api.ApplicationConstants.Environment
 [ERROR] /Work/tachyon/upstream/integration/yarn/src/main/java/tachyon/yarn/Client.java:[307,31] cannot find symbol
 [ERROR] symbol:   variable CLASS_PATH_SEPARATOR
 [ERROR] location: interface org.apache.hadoop.yarn.api.ApplicationConstants
 [ERROR] /tachyon/upstream/integration/yarn/src/main/java/tachyon/yarn/Client.java:[310,29] cannot find symbol
 [ERROR] symbol:   variable CLASS_PATH_SEPARATOR
 [ERROR] location: interface org.apache.hadoop.yarn.api.ApplicationConstants
 [ERROR] /tachyon/upstream/integration/yarn/src/main/java/tachyon/yarn/Client.java:[312,47] cannot find symbol
 [ERROR] symbol:   variable CLASS_PATH_SEPARATOR
 [ERROR] location: interface org.apache.hadoop.yarn.api.ApplicationConstants
 [ERROR] /tachyon/upstream/integration/yarn/src/main/java/tachyon/yarn/Client.java:[314,47] cannot find symbol
 [ERROR] symbol:   variable CLASS_PATH_SEPARATOR
 [ERROR] location: interface org.apache.hadoop.yarn.api.ApplicationConstants
 [ERROR] -> [Help 1]
 ```

Please make sure you are using the proper hadoop version
```bash
$ mvn clean install -Dhadoop.version=2.4.1 -Pyarn
```
