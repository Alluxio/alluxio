---
layout: global
title: CephObjectStorage
---


This guide describes how to configure Alluxio with [Ceph Object Storage](https://ceph.com/en/discover/technology/#object){:target="_blank"} as the under storage system. 

Ceph Object Storage is a distributed, open-source storage system designed for storing and retrieving large amounts of unstructured data. It provides a scalable and highly available storage solution that can be deployed on commodity hardware.

Alluxio supports two different clients APIs to connect to Ceph Object Storage using [Rados Gateway](http://docs.ceph.com/docs/master/radosgw/){:target="_blank"}. For more information, please read its documentation:
- [S3](https://docs.ceph.com/en/latest/radosgw/s3/){:target="_blank"} (preferred)

## Prerequisites

If you haven't already, please see [Prerequisites]({{ '/en/ufs/Storage-Overview.html#prerequisites' | relativize_url }}) before you get started.

In preparation for using Ceph Object Storage with Alluxio:

<table class="table table-striped">
    <tr>
        <td markdown="span" style="width:30%">`<CEPH_BUCKET>`</td>
        <td markdown="span">[Create a new S3 bucket](https://docs.ceph.com/en/quincy/radosgw/s3/bucketops/){:target="_blank"} or use an existing bucket</td>
    </tr>
    <tr>
        <td markdown="span" style="width:30%">`<CEPH_DIRECTORY>`</td>
        <td markdown="span">The directory you want to use in the bucket, either by creating a new directory or using an existing one</td>
    </tr>
    <tr>
        <td markdown="span" style="width:30%">`<S3_ACCESS_KEY_ID>`</td>
        <td markdown="span">Used to sign programmatic requests made to AWS. See [How to Obtain Access Key ID and Secret Access Key](https://docs.aws.amazon.com/powershell/latest/userguide/pstools-appendix-sign-up.html){:target="_blank"}</td>
    </tr>
    <tr>
        <td markdown="span" style="width:30%">`<S3_SECRET_KEY>`</td>
        <td markdown="span">Used to sign programmatic requests made to AWS. See [How to Obtain Access Key ID and Secret Access Key](https://docs.aws.amazon.com/powershell/latest/userguide/pstools-appendix-sign-up.html){:target="_blank"}</td>
    </tr>
    <tr>
        <td markdown="span" style="width:30%">`<RGW_HOSTNAME>`</td>
        <td markdown="span">The host for the Ceph Object Gateway instance, which can be an IP address or a hostname. Read [more](https://docs.ceph.com/en/mimic/radosgw/config-ref/){:target="_blank"}</td>
    </tr>
    <tr>
        <td markdown="span" style="width:30%">`<RGW_PORT>`</td>
        <td markdown="span">The port the instance listens for requests and if not specified, Ceph Object Gateway runs external FastCGI. Read [more](https://docs.ceph.com/en/mimic/radosgw/config-ref/){:target="_blank"}</td>
    </tr>
    <tr>
        <td markdown="span" style="width:30%">`<INHERIT_ACL>`</td>
        <td markdown="span"></td>
    </tr>
</table>


## Basic Setup

To use Ceph Object Storage as the UFS of Alluxio root mount point, you need to configure Alluxio to use under storage systems by modifying `conf/alluxio-site.properties`. If it does not exist, create the configuration file from the template.

```shell
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Modify `conf/alluxio-site.properties` to include:

```properties
alluxio.dora.client.ufs.root=s3://<CEPH_BUCKET>/<CEPH_DIRECTORY>
s3a.accessKeyId=<S3_ACCESS_KEY_ID>
s3a.secretKey=<S3_SECRET_KEY>
alluxio.underfs.s3.endpoint=http://<RGW-HOSTNAME>:<RGW-PORT>
alluxio.underfs.s3.disable.dns.buckets=true
alluxio.underfs.s3.inherit.acl=<INHERIT_ACL>
```

If using a Ceph release such as hammer (or older) specify `alluxio.underfs.s3.signer.algorithm=S3SignerType`
to use v2 S3 signatures. To use GET Bucket (List Objects) Version 1 specify
`alluxio.underfs.s3.list.objects.v1=true`.

## Running Alluxio Locally with Ceph

Once you have configured Alluxio to Ceph Object Storage, try [running Alluxio locally]({{ '/en/ufs/Storage-Overview.html#running-alluxio-locally' | relativize_url}}) to see that everything works.

## Advanced Setup

### Access Control

If Alluxio security is enabled, Alluxio enforces the access control inherited from underlying Ceph
Object Storage. Depending on the interace used, refer to
[S3 Access Control]({{ '/en/ufs/S3.html' | relativize_url }}#identity-and-access-control-of-s3-objects) or
