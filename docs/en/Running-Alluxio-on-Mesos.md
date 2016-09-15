Alluxio can be deployed through Mesos. This allows Mesos to manage the resources used by Alluxio. The alternative to deploying Alluxio through Mesos is to install Alluxio separately and subtract the memory it uses from the pool of memory managed by Mesos.

## Mesos version

Alluxio is compatible with Mesos 0.23.0 and later.

## Deploying Alluxio on Mesos

To deploy Alluxio on Mesos, we need to make the Alluxio distribution available to Mesos. There are two ways to do this:

1. Install Alluxio on all Mesos nodes.
2. Point Mesos to an Alluxio tarball.

#### Deploy with Alluxio already installed on all Mesos nodes

1. Install Alluxio on all Mesos nodes

  Now from anywhere with Alluxio installed:
2. Set the configuration property "alluxio.integration.mesos.alluxio.jar.url" to "LOCAL"
3. Set the configuration property "alluxio.home" to the path where Alluxio is installed on the Mesos nodes
4. Launch the Alluxio Mesos framework
  ```bash
  ./integration/bin/alluxio-mesos.sh mesosMaster:5050 // address of Mesos master
  ```

#### Deploy with Alluxio tarball url

From anywhere with Alluxio installed:
1. Set the configuration property "alluxio.integration.mesos.alluxio.jar.url" to point to an Alluxio tarball
2. Launch the Alluxio Mesos framework
  ```bash
  ./integration/bin/alluxio-mesos.sh mesosMaster:5050 // address of Mesos master
  ```

Note that the tarball should be compiled with -Pmesos. Released Alluxio tarballs from version 1.3.0 onwards are compiled this way.

#### Java

By default, the Alluxio Mesos framework will download the Java 7 jdk and use it to run Alluxio. If you would prefer
to use whatever version of java is available on the Mesos executor, set the configuration property
```
alluxio.integration.mesos.jdk.url="LOCAL"
```
