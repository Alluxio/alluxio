---
layout: global
title: Storage Integrations Overview
---

This guide will cover general prerequisites and running Alluxio locally with your desired under storage system. To learn how to configure Alluxio with each individual storage system, please look at their respective pages.

## Prerequisites

In preparation for using your chosen storage system with Alluxio, please be sure you have all the required location, credentials, and additional properties before you begin configuring Alluxio to your under storage system.

For the purposes of this guide, the following are placeholders.

<table class="table table-striped">
    <tr>
        <th>Storage System</th>
        <th>Location</th>
        <th>Credentials</th>
        <th>Additional Properties</th>
    </tr>
    <tr>
        <td markdown="span">[Amazon AWS S3]({{ '/en/ufs/S3.html' | relativize_url }})</td>
        <td markdown="span">`S3_BUCKET`, `S3_DIRECTORY`</td>
        <td markdown="span">`S3_ACCESS_KEY_ID`, `S3_SECRET_KEY`</td>
        <td markdown="span"></td>
    </tr>
    <tr>
        <td markdown="span">[HDFS]({{ '/en/ufs/HDFS.html' | relativize_url }})</td>
        <td markdown="span">`HDFS_NAMENODE`, `HDFS_PORT`</td>
        <td markdown="span"></td>
        <td markdown="span">
            Specify Hadoop version: <br />
            `HADOOP_VERSION`</td>
    </tr>
    <tr>
        <td markdown="span">[Aliyun Object Storage Service (OSS)]({{ '/en/ufs/Aliyun-OSS.html' | relativize_url }})</td>
        <td markdown="span">`OSS_BUCKET`, `OSS_DIRECTORY`</td>
        <td markdown="span">`OSS_ACCESS_KEY_ID`, `OSS_ACCESS_KEY_SECRET`, `OSS_ENDPOINT`</td>
        <td markdown="span"></td>
    </tr>
    <tr>
        <td markdown="span">[Azure Blob Store]({{ '/en/ufs/Azure-Blob-Store.html' | relativize_url }})</td>
        <td markdown="span">`AZURE_CONTAINER`, `AZURE_DIRECTORY`</td>
        <td markdown="span">`AZURE_ACCOUNT`, `AZURE_ACCOUNT_KEY`</td>
        <td markdown="span"></td>
    </tr>
    <tr>
        <td markdown="span">[Azure Data Lake Storage Gen1]({{ '/en/ufs/Azure-Data-Lake.html' | relativize_url }})</td>
        <td markdown="span">`AZURE_DIRECTORY`</td>
        <td markdown="span">`AZURE_ACCOUNT`</td>
        <td markdown="span">OAuth credentials: <br />
            `CLIENT_ID`, `AUTHENTICATION_KEY`, `TENANT_ID`</td>
    </tr>
    <tr>
        <td markdown="span">[Azure Data Lake Storage Gen2]({{ '/en/ufs/Azure-Data-Lake-Gen2.html' | relativize_url }})</td>
        <td markdown="span">`AZURE_CONTAINER`, `AZURE_DIRECTORY`</td>
        <td markdown="span">`AZURE_ACCOUNT`, `AZURE_SHARED_KEY`</td>
        <td markdown="span">
            OAuth credentials: <br />
            `OAUTH_ENDPOINT`, `CLIENT_ID`, `CLIENT_SECRET`, `MSI_ENDPOINT`, `MSI_TENANT`</td>
    </tr>
    <tr>
        <td markdown="span">[CephFS]({{ '/en/ufs/CephFS.html' | relativize_url }})</td>
        <td markdown="span"></td>
        <td markdown="span">`CEPHFS_CONF_FILE`, `CEPHFS_NAME`, `CEPHFS_DIRECTORY`, `CEPHFS_AUTH_ID`, `CEPHFS_KEYRING_FILE`</td>
        <td markdown="span"></td>
    </tr>
    <tr>
        <td markdown="span">[CephObjectStorage]({{ '/en/ufs/CephObjectStorage.html' | relativize_url }})</td>
        <td markdown="span">`CEPH_BUCKET`, `CEPH_DIRECTORY`</td>
        <td markdown="span">
            [S3](http://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html) (preferred): `S3_ACCESS_KEY_ID`, `S3_SECRET_KEY_ID` <br />
        </td>
        <td markdown="span">
            Specify S3 properties: (preferred) <br />
            `RGW_HOSTNAME`, `RGW_PORT`, `INHERIT_ACL` <br /><br />
        </td>
    </tr>
    <tr>
        <td markdown="span">[Google Cloud Storage (GCS)]({{ '/en/ufs/GCS.html' | relativize_url }})</td>
        <td markdown="span">`GCS_BUCKET`, `GCS_DIRECTORY`</td>
        <td markdown="span">For GCS Version 1: `GCS_ACCESS_KEY_ID`, `GCS_SECRET_ACCESS_KEY`</td>
        <td markdown="span"></td>
    </tr>
    <tr>
        <td markdown="span">[Huawei Object Storage Service (OBS)]({{ '/en/ufs/Huawei-OBS.html' | relativize_url }})</td>
        <td markdown="span">`OBS_BUCKET`, `OBS_DIRECTORY`</td>
        <td markdown="span">`OBS_ACCESS_KEY`, `OBS_SECRET_KEY`, `OBS_ENDPOINT`</td>
        <td markdown="span"></td>
    </tr>
    <tr>
        <td markdown="span">[MinIO]({{ '/en/ufs/Minio.html' | relativize_url }})</td>
        <td markdown="span">`MINIO_BUCKET`, `MINIO_DIRECTORY`</td>
        <td markdown="span">`S3_ACCESS_KEY_ID`, `S3_SECRET_KEY`, `MINIO_ENDPOINT`</td>
        <td markdown="span"></td>
    </tr>
    <tr>
        <td markdown="span">[NFS]({{ '/en/ufs/NFS.html' | relativize_url }})</td>
        <td markdown="span"></td>
        <td markdown="span"></td>
        <td markdown="span"></td>
    </tr>
    <tr>
        <td markdown="span">[Ozone]({{ '/en/ufs/Ozone.html' | relativize_url }})</td>
        <td markdown="span">
            [o3fs](https://ozone.apache.org/docs/1.0.0/interface/ofs.html): `OZONE_BUCKET`, `OZONE_VOLUME` <br />
            [ofs](https://ozone.apache.org/docs/1.0.0/interface/o3fs.html): `OZONE_MANAGER`, `OZONE_BUCKET`, `OZONE_DIRECTORY`, `OZONE_VOLUME`</td>
        <td markdown="span">
            `OM_SERVICE_IDS`</td>
        <td markdown="span">
            Mount specific version: <br />
            `OZONE_VERSION`</td>
    </tr>
    <tr>
        <td markdown="span">[Tencent Cloud Object Storage (COS)]({{ '/en/ufs/Tencent-COS.html' | relativize_url }})</td>
        <td markdown="span">`COS_BUCKET`, `COS_DIRECTORY`</td>
        <td markdown="span">`COS_ACCESS_KEY`, `COS_SECRET_KEY`</td>
        <td markdown="span">
            Specify COS region: <br />
            `COS_REGION`, `COS_APPID`
        </td>
    </tr>
    <tr>
        <td markdown="span">[Tencent Cloud Object Storage in Hadoop (COSN)]({{ '/en/ufs/Tencent-COS.html' | relativize_url }})</td>
        <td markdown="span">`COSN_BUCKET`, `COSN_DIRECTORY`</td>
        <td markdown="span">`COSN_SECRET_ID`, `COSN_SECRET_KEY`</td>
        <td markdown="span">
            Specify COSN region: <br />
            `COSN_REGION`
        </td>
    </tr>
</table>

## Running Alluxio Locally

Once you have configured Alluxio to your desired under storage system, start up Alluxio locally to see that everything works.

```shell
$ ./bin/alluxio format
$ ./bin/alluxio-start.sh local
```

This should start an Alluxio master and an Alluxio worker. You can see the master UI at
[http://localhost:19999](http://localhost:19999).

Run a simple example program:

```shell
$ ./bin/alluxio runTests
```

Visit your container `<CONTAINER>/<DIRECTORY>` or bucket `<BUCKET>/<DIRECTORY>` to verify the files and directories created by Alluxio exist. If there are no errors, then you have successfully configured your storage system!

To stop Alluxio, you can run:

``` shell
$ ./bin/alluxio-stop.sh local
```