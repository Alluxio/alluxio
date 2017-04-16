---
layout: global
title: Configuring Alluxio with Amazon S3
nickname: Alluxio with S3
group: Under Store
priority: 0
---

* Table of Contents
{:toc}

This guide describes how to configure Alluxio with [Amazon S3](https://aws.amazon.com/s3/) as the
under storage system. Alluxio natively provides two different client implementations for accessing
s3, aws-sdk-java-s3 through the s3a:// scheme (recommended for better performance) and jets3t
through the s3n:// scheme.

## Initial Setup

First, the Alluxio binaries must be on your machine. You can either
[compile Alluxio](Building-Alluxio-Master-Branch.html), or
[download the binaries locally](Running-Alluxio-Locally.html).

Then, if you haven't already done so, create your configuration file with `bootstrapConf` command.
For example, if you are running Alluxio on your local machine, `ALLUXIO_MASTER_HOSTNAME` should be
set to `localhost`

{% include Configuring-Alluxio-with-S3/bootstrapConf.md %}

Alternatively, you can also create the configuration file from the template and set the contents
manually.

{% include Common-Commands/copy-alluxio-env.md %}

Also, in preparation for using S3 with Alluxio, create a bucket (or use an existing bucket). You
should also note the directory you want to use in that bucket, either by creating a new directory in
the bucket, or using an existing one. For the purposes of this guide, the S3 bucket name is called
`S3_BUCKET`, and the directory in that bucket is called `S3_DIRECTORY`.

## Configuring Alluxio

You need to configure Alluxio to use S3 as its under storage system by modifying
`conf/alluxio-site.properties`. The first modification is to specify an **existing** S3
bucket and directory as the under storage system. You specify it by modifying
`conf/alluxio-site.properties` to include:

{% include Configuring-Alluxio-with-S3/underfs-address-s3n.md %}

or

{% include Configuring-Alluxio-with-S3/underfs-address-s3a.md %}

Next, you need to specify the AWS credentials for S3 access.

If you are using s3n, in `conf/alluxio-site.properties`, add:

{% include Configuring-Alluxio-with-S3/aws.md %}

Here, `<AWS_ACCESS_KEY_ID>` and `<AWS_SECRET_ACCESS_KEY>` should be replaced with your actual
[AWS keys](https://aws.amazon.com/developers/access-keys), or other environment variables that
contain your credentials.

If you are using s3a, you can specify credentials in 4 ways, from highest to lowest priority:

* Environment Variables `AWS_ACCESS_KEY_ID` or `AWS_ACCESS_KEY` (either is acceptable) and `AWS_SECRET_ACCESS_KEY` or `AWS_SECRET_KEY` (either is acceptable)
* System Properties `aws.accessKeyId` and `aws.secretKey`
* Profile file containing credentials at `~/.aws/credentials`
* AWS Instance profile credentials, if you are using an EC2 instance

See [Amazon's documentation](http://docs.aws.amazon.com/java-sdk/latest/developer-guide/credentials.html#id6)
for more details.

Alternatively, these configuration settings can be set in the `conf/alluxio-env.sh` file. More
details about setting configuration parameters can be found in
[Configuration Settings](Configuration-Settings.html#environment-variables).

### Enabling Server Side Encryption

If you are using s3a, you may encrypt your data stored in S3. The encryption is only valid for data
at rest in s3 and will be transferred in decrypted form when read by clients.

Enable this feature by configuring `conf/alluxio-site.properties`:

{% include Configuring-Alluxio-with-S3/server-side-encryption-conf.md %}

### Disable DNS-Buckets

The underlying S3 library JetS3t can incorporate bucket names that are DNS-compatible into the host
name of its requests. You can optionally configure this behavior in the `ALLUXIO_JAVA_OPTS` section
of the `conf/alluxio-site.properties` file by adding:

{% include Configuring-Alluxio-with-S3/jets3t.md %}

With `<DISABLE_DNS>` replaced with `false` (the default), a request directed at the bucket named
"mybucket" will be sent to the host name "mybucket.s3.amazonaws.com". With `<DISABLE_DNS>` replaced
with `true`, JetS3t will specify bucket names in the request path of the HTTP message rather than
the Host header, for example: "http://s3.amazonaws.com/mybucket". Without this parameter set, the
system will default to `false`. See http://www.jets3t.org/toolkit/configuration.html for further
details.

After these changes, Alluxio should be configured to work with S3 as its under storage system, and
you can try [Running Alluxio Locally with S3](#running-alluxio-locally-with-s3).

### Accessing S3 through a proxy

To communicate with S3 through a proxy, modify `conf/alluxio-site.properties` to include:

{% include Configuring-Alluxio-with-S3/proxy.md %}

Here, `<PROXY_HOST>` and `<PROXY_PORT>` should be replaced the host and port for your proxy, and
`<USE_HTTPS?>` should be set to either `true` or `false`, depending on whether https should be
used to communicate with the proxy.

These configuration parameters may also need to be set for the Alluxio client if it is running in
a separate JVM from the Alluxio Master and Workers. See
[Configuring Distributed Applications](#configuring-distributed-applications)

## Configuring Application Dependency

When building your application to use Alluxio, your application will have to include the
`alluxio-core-client` module. If you are using [maven](https://maven.apache.org/), you can add the
dependency to your application with:

{% include Configuring-Alluxio-with-S3/dependency.md %}

Alternatively, you may copy `conf/alluxio-site.properties` (having the properties setting
credentials) to the classpath of your application runtime (e.g., `$SPARK_CLASSPATH` for Spark), or
append the path to this site properties file to the classpath.

### Avoiding Conflicting Client Dependencies

The jets3t and aws-sdk s3 clients all have dependencies on common libraries such as HTTP libraries.
These dependencies are usually not in conflict with other projects, but in cases like using Apache
MapReduce with the S3A client, conflicting versions may cause issues at runtime. You can resolve
this conflict enabling ufs delegation, `alluxio.user.ufs.delegation.enabled=true`, which delegates
client operations to the under storage through Alluxio servers. See
[Configuration Settings](Configuration-Settings.html) for how to modify the Alluxio configuration.
Alternatively you can manually resolve the conflicts when generating the MapReduce classpath and/or
jars, keeping only the highest versions of each dependency.

### Enabling the Hadoop S3 Client (instead of the native S3 client)

Alluxio provides a native client to communicate with S3. By default, the native S3 client is used
when Alluxio is configured to use S3 as its under storage system.

However, there is also an option to use a different implementation to communicate with S3; the S3
client provided by Hadoop. In order to disable the Alluxio S3 client (and enable the Hadoop S3
client), additional modifications to your application must be made. When including the
`alluxio-core-client` module in your application, the `alluxio-underfs-s3` should be excluded to
disable the native client, and to use the Hadoop S3 client:

{% include Configuring-Alluxio-with-S3/hadoop-s3-dependency.md %}

However, the Hadoop S3 client needs the `jets3t` package in order to use S3, but it is not included
as a dependency automatically. Therefore, you must also add the `jets3t` dependency manually. When
using maven, you can add the following to pull in the `jets3t` dependency:

{% include Configuring-Alluxio-with-S3/jets3t-dependency.md %}

The `jets3t` version `0.9.0` works for Hadoop version `2.3.0`. The `jets3t` version `0.7.1` should
work for older versions of Hadoop. To find the exact `jets3t` version for your Hadoop version,
please refer to [MvnRepository](http://mvnrepository.com/).

### Using a non-Amazon service provider

To use an S3 service provider other than "s3.amazonaws.com", modify `conf/alluxio-site.properties`
to include:

{% include Configuring-Alluxio-with-S3/non-amazon.md %}

For these parameters, replace `<S3_ENDPOINT>` with the host name of your S3 service. Only use this
parameter if you are using a provider other than `s3.amazonaws.com`.

Replace `<USE_HTTPS>` with `true` or `false`. If `true` (using HTTPS), also replace `<HTTPS_PORT>`,
with the HTTPS port for the provider and remove the `alluxio.underfs.s3.endpoint.http.port`
parameter. If you replace `<USE_HTTPS>` with `false` (using HTTP) also replace `<HTTP_PORT>` with
the HTTP port for the provider, and remove the `alluxio.underfs.s3.endpoint.https.port` parameter.
If the HTTP or HTTPS port values are left unset, `<HTTP_PORT>` defaults to port 80, and
`<HTTPS_PORT>` defaults to port 443.

### Using v2 S3 Signatures

Some S3 service providers only support v2 signatures. For these S3 providers, you can enforce using
the v2 signatures by setting the `alluxio.underfs.s3a.signer.algorithm` to `S3SignerType`.

### Configuring Distributed Applications Runtime

When I/O is delegated to Alluxio workers (i.e., Alluxio configuration
`alluxio.user.ufs.operation.delegation` is true, which is false by default since Alluxio 1.1), you
do not have to do any thing special for your applications. Otherwise since you are using an Alluxio
client that is running separately from the Alluxio Master and Workers (in a separate JVM), then you
need to make sure that your AWS credentials are provided to the application JVM processes as well.
The easiest way to do this is to add them as command line options when starting your client JVM
process. For example:

{% include Configuring-Alluxio-with-S3/java-bash.md %}

## Running Alluxio Locally with S3

After everything is configured, you can start up Alluxio locally to see that everything works.

{% include Common-Commands/start-alluxio.md %}

This should start an Alluxio master and an Alluxio worker. You can see the master UI at
[http://localhost:19999](http://localhost:19999).

Next, you can run a simple example program:

{% include Common-Commands/runTests.md %}

After this succeeds, you can visit your S3 directory `S3_BUCKET/S3_DIRECTORY` to verify the files
and directories created by Alluxio exist. For this test, you should see files named like:

{% include Configuring-Alluxio-with-S3/s3-file.md %}

To stop Alluxio, you can run:

{% include Common-Commands/stop-alluxio.md %}

## S3 Access Control

If Alluxio security is enabled, Alluxio enforces the access control inherited from underlying object
storage.

The S3 credentials specified in Alluxio config represents a S3 user. S3 service backend checks the
user permission to the bucket and the object for access control. If the given S3 user does not have
the right access permission to the specified bucket, a permission denied error will be thrown. When
Alluxio security is enabled, Alluxio loads the bucket ACL to Alluxio permission on the first time
when the metadata is loaded to Alluxio namespace.

### Mapping from S3 user to Alluxio file owner

By default, Alluxio tries to extract the S3 user display name from the S3 credential. Optionally,
`alluxio.underfs.s3.owner.id.to.username.mapping` can be used to specify a preset S3 canonical id to
Alluxio username static mapping, in the format "id1=user1;id2=user2".  The AWS S3 canonical ID can
be found at the console [address](https://console.aws.amazon.com/iam/home?#security_credential).
Please expand the "Account Identifiers" tab and refer to "Canonical User ID".

### Mapping from S3 ACL to Alluxio permission

Alluxio checks the S3 bucket READ/WRITE ACL to determine the owner's permission mode to a Alluxio
file. For example, if the S3 user has read-only access to the underlying bucket, the mounted
directory and files would have 0500 mode. If the S3 user has full access to the underlying bucket,
the mounted directory and files would have 0700 mode.

### Mount point sharing

If you want to share the S3 mount point with other users in Alluxio namespace, you can enable
`alluxio.underfs.object.store.mount.shared.publicly`.

### Permission change

In addition, chown/chgrp/chmod to Alluxio directories and files do NOT propagate to the underlying
S3 buckets nor objects.
