---
layout: global
title: Configuring Alluxio with OrangeFS
nickname: Alluxio with OrangeFS
group: Under Store
priority: 2
---

This guide describes how to configure Alluxio with [OrangeFS](http://www.orangefs.org/) as the 
under storage system.

# Building Alluxio-OrangeFS Distro

The integration of Alluxio-OrangeFS depends on 
[orangefs-jni](http://www.orangefs.org/fisheye/orangefs/browse/orangefs/branches/maven-repository/maven2/org/orangefs/usrint/orangefs-jni) 
for an optimised I/O performance that is comparable to OrangeFS 
[native performance](http://docs.orangefs.com/v_2_9/Design_Overview.htm).

Due to licensing issues with runtime proprietary binaries, we do not include the `orangefs-jni` 
dependency by default. To build an integrated distro, you need to either include the 
orangefs-jni.jar that comes with 
[OrangeFS distribution](http://docs.orangefs.com/v_2_9/HPC_Setup_.htm#Install_System_Software) 
or compile Alluxio with maven option `-Porangefs-lgpl -Dorangefs.version=2.9.4` as a dependency 
of your project. To build OrangeFS against Alluxio Master branch, you can reference 
[compile Alluxio](Building-Alluxio-Master-Branch.html).

Then, if you haven't already done so, create your configuration file from the template:

{% include Common-Commands/copy-alluxio-env.md %}

# Configuring Alluxio

To configure Alluxio to use OrangeFS as its under storage system, modifications to the 
`conf/alluxio-env.sh` or `alluxio-site.properties` file must be made. The first modification 
is to specify an existing OrangeFS file system as the under storage system. You can specify 
it by modifying `conf/alluxio-env.sh` to include:

{% include Configuring-Alluxio-with-OrangeFS/underfs-address.md %}
    
Next you need to specify mount points and other optional properties for the OrangeFS file system. 
In the ALLUXIO_JAVA_OPTS section of the `conf/alluxio-env.sh` file, add:

{% include Configuring-Alluxio-with-OrangeFS/ofs-access.md %}
    
Here, `<OFS_MOUNT_POINTS>` are the root directories of OrangeFS file systems. They should share 
the same names as defined in OrangeFS 
[pvfs2tab file](http://docs.orangefs.com/v_2_9/pvfs2tab_File.htm). `<OFS_LAYOUT>` and 
`<OFS_BLOCK_SIZE>` are two optional 
[hints](http://www.orangefs.org/trac/orangefs/wiki/Distributions) properties, which are used to 
determine how a Alluxio data file is distributed to underlying OrangeFS servers. 
`<OFS_BUFFER_SIZE>` is used to define the data request size between Alluxio and OrangeFS.  

If you feel not sure about how to change the `conf/alluxio-env.sh`, there is another way to 
provide these configurations. You can provide a properties config file named : 
`alluxio-site.properties` in the `conf/` directory, and edit it as below:

{% include Configuring-Alluxio-with-OrangeFS/properties.md %}

After these changes, Alluxio should be configured to work with OrangeFS as its under storage 
system, and you can try to run alluxio locally with OrangeFS.

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
