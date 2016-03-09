---
layout: global
title: Configuring Alluxio with OrangeFS
nickname: Alluxio with OrangeFS
group: Under Store
priority: 2
---

This guide describes how to configure Alluxio with [OrangeFS](http://www.orangefs.org/) as the under
storage system.

# Initial Setup

First, the Alluxio binaries must be on your machine. You can either
[compile Alluxio](Building-Alluxio-Master-Branch.html), or
[download the binaries locally](Running-Alluxio-Locally.html).

Then, if you haven't already done so, create your configuration file from the template:

{% include Common-Commands/copy-alluxio-env.md %}

# Configuring Alluxio

To configure Alluxio to use OrangeFS as its under storage system, modifications to the `conf/alluxio-env.sh` or `alluxio-site.properties` file must be made. The first modification is to specify an existing OrangeFS file system as the under storage system. You can specify it by modifying `conf/alluxio-env.sh` to include:

{% include Configuring-Alluxio-with-OrangeFS/underfs-address.md %}
    
Next you need to specify mount points and other optional properties for the OrangeFS file system. In the ALLUXIO_JAVA_OPTS section of the `conf/alluxio-env.sh` file, add:

{% include Configuring-Alluxio-with-OrangeFS/ofs-access.md %}
    
Here, `<OFS_MOUNT_POINTS>` is/are the root directory/directories of OrangeFS file system(s). It/They should have the same name(s) as defined in OrangeFS [pvfs2tab file](http://docs.orangefs.com/v_2_9/pvfs2tab_File.htm). `<OFS_LAYOUT>` and `<OFS_BLOCK_SIZE>` are two optional [hints](http://www.orangefs.org/trac/orangefs/wiki/Distributions) properties, which are used to determine how a Alluxio data file is distributed to underlying OrangeFS servers. `<OFS_BUFFER_SIZE>` is used to define the data request size between Alluxio and OrangeFS.  

If you feel not sure about how to change the `conf/alluxio-env.sh`, there is another way to provide these configurations. You can provide a properties config file named : `alluxio-site.properties` in the `conf/` directory, and edit it as below:

{% include Configuring-Alluxio-with-OrangeFS/properties.md %}

After these changes, Alluxio should be configured to work with OrangeFS as its under storage system, and you can try to run alluxio locally with OrangeFS.

# Running Alluxio Locally with OrangeFS

After everything is configured, you can start up Alluxio locally to see that everything works.

{% include Common-Commands/start-alluxio.md %}

This should start an Alluxio master and an Alluxio worker. You can see the master UI at
[http://localhost:19999](http://localhost:19999).

Next, you can run a simple example program:

{% include Common-Commands/runTests.md %}

After this succeeds, you can visit your OrangeFS volume to verify the files and directories created
by Alluxio exist. For this test, you should see files named like:

{% include Configuring-Alluxio-with-OrangeFS/glusterfs-file.md %}

To stop Alluxio, you can run:

{% include Common-Commands/stop-alluxio.md %}
