Alluxio can be deployed through Mesos. This allows Mesos to manage the resources used by Alluxio.

## Mesos version

Alluxio is compatible with Mesos 0.23.0 and later.

## Deploying Alluxio on Mesos

To deploy Alluxio on Mesos, we need to make the Alluxio distribution available to Mesos. There are two ways to do this:

1. Install Alluxio on all Mesos nodes.
2. Point Mesos to an Alluxio tarball.

#### Setting configuration properties
See the [Configuration Settings](Configuration-Settings.html) documentation for instructions on how to set configuration properties.

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

#### Configuring Alluxio Masters and Workers

When Alluxio is deployed on Mesos, it propagates all Alluxio configuration to the launched masters and workers.
This means you can configure the launched Alluxio cluster by setting configuration properties in `conf/alluxio-site.properties`.

#### Log files

The `./integration/bin/alluxio-mesos.sh` script will launch an `AlluxioFramework` Java process which will log to `logs/framework.out`.
Alluxio masters and workers launched on Mesos will write their Alluxio logs to `logs/` inside the root Mesos container directory. There
may also be useful information in the `stderr` file.
