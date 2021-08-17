---
layout: global
title: Running Alluxio On Tencent Cloud EMR
nickname: Tencent EMR
group: Cloud Native
priority: 3
---

This guide describes how to configure Alluxio to run on [Tencent Cloud EMR](https://cloud.tencent.com/product/emr).

* Table of Contents
{:toc}

## Overview
The out-of-the-box Alluxio service provided on Tencent Cloud EMR can help customers quickly achieve distributed memory-level cache acceleration while simplifying data management. At the same time, the Alluxio service can quickly configure multi-level caches and manage metadata by operating the EMR console or API interface using the configuration delivery function, as well as the ability to obtain one-stop monitoring and alarms.


## Prerequisites
- Hadoop Standard version of Tencent Cloud EMR >= 2.1.0
- Hadoop TianQiong version of Tencent Cloud EMR >= 1.0
- For the specific Alluxio version supported in EMR, please refer to [Component Version supported Tencent Cloud EMR](https://intl.cloud.tencent.com/document/product/1026/31095).

## Create EMR cluster based on Alluxio
This part mainly explains how to create an out-of-the-box Alluxio cluster on Tencent Cloud EMR. EMR provides two ways to build a cluster using the WEB purchase page and API creation:

{% accordion create %}

  {% collapsible Cluster details Create a cluster on the purchase page %}

You need to log in to the [Tencent Cloud EMR purchase page](https://buy.cloud.tencent.com/emapreduce/), select the supported Alluxio release version on the purchase page, and check the Alluxio component in the optional component list
  ![](https://application-display-1259353343.cos.ap-hongkong.myqcloud.com/alluxio/alluxio-create.png)
Other options can be customized according to specific business scenarios. For specific options during the creation process, please refer to [here](https://intl.cloud.tencent.com/document/product/1026/31097)
  {% endcollapsible %}
  
  {% collapsible Create a cluster use API %}
At the same time, Tencent Cloud EMR also provides [API](https://intl.cloud.tencent.com/document/product/1026/31045) to build a big data cluster based on Alluxio.

  {% endcollapsible %}

{% endaccordion %}

## Basic configuration
Created a Tencent Cloud EMR with Alluxio components. By default, HDFS will be mounted on Alluxio, and memory will be used as a single level 0 storage. If you need to change the multi-level storage and other optimization items that are more in line with the business characteristics, you can use the configuration delivery function to complete the related configuration:
  ![](https://application-display-1259353343.cos.ap-hongkong.myqcloud.com/alluxio/alluxio-config.png)
  
After the configuration is delivered, some configurations need to restart the Alluxio service to take effect:
  ![](https://application-display-1259353343.cos.ap-hongkong.myqcloud.com/alluxio/alluxio-restart.png)
  
For more details on the configuration issuance and restart strategy, please refer to related documents:

- [Modifying Component Parameters](https://intl.cloud.tencent.com/document/product/1026/31109)

- [Restart the component](https://intl.cloud.tencent.com/document/product/1026/31110)

## Accelerate computing and storage separation based on Alluxio
Tencent Cloud EMR provides computing and storage separation capabilities based on Tencent Cloud Object Storage (COS). By default, when directly accessing data in the object storage, the application does not have node-level data locality or cross-application caching. Using Alluxio to accelerate will alleviate these problems.
On the Tencent Cloud EMR cluster, COS has been deployed by default as the dependent jar package of UFS. You only need to authorize access to COS and mount COS to Alluxio to use it.

{% accordion accelerate %}
  {% collapsible Authorization %}
If the object storage is not enabled in the current cluster, you can click Authorize to authorize. After authorization, the nodes in the EMR can access the data in the COS through the temporary key.
  ![](https://application-display-1259353343.cos.ap-hongkong.myqcloud.com/alluxio/cos-auth.png)
  
  ![](https://application-display-1259353343.cos.ap-hongkong.myqcloud.com/alluxio/auth-confirm.png)

  {% endcollapsible %}
  
{% endaccordion %}

For more details on using Alluxio development in Tencent Cloud EMR, please refer to [here](https://intl.cloud.tencent.com/document/product/1026/31168).
