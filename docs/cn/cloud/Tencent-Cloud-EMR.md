---
layout: global
title: 在腾讯云EMR中使用 Alluxio
nickname: Tencent EMR
group: Cloud Native
priority: 3
---

* 内容列表
{:toc}


## 概述

在腾讯云EMR上提供了开箱可用的Alluxio服务，以帮助腾讯云客户可以快速实现分布式内存级缓存加速，简化数据管理等能力；同时还可以通过腾讯云EMR控制台或API接口，使用配置下发功能力快速配置多层级缓存和元数据管理等；获取一站式监控告警能力等能力。


## 准备

- 腾讯云EMR的Hadoop标准版本>=2.1.0
- 腾讯云EMR的Hadoop天穹版本>=1.0
- 有关EMR中版本中支持具体的Alluxio的版本支持可参考[这里](https://cloud.tencent.com/document/product/589/20279)

## 创建基于Alluxio的EMR集群

这部分主要说明如何在腾讯云EMR上创建开箱即用的Alluxio集群。EMR提供了使用WEB购买页创建和API创建两种方式来构建集群：

{% accordion create %}

  {% collapsible 购买页创建集群 %}
  
您需要登陆腾讯云 [EMR购买页](https://buy.cloud.tencent.com/emapreduce/) ，在购买页选择支持的Alluxio发布版本，并且在可选组件列表中勾选上Alluxio组件.

  ![](https://application-display-1259353343.cos.ap-hongkong.myqcloud.com/alluxio/alluxio-create_cn.png)

其他的选项可根据业务具体业务场景，进行个性化配置，创建过程中的具体选项可参考[这里](https://cloud.tencent.com/document/product/589/10981)

  {% endcollapsible %}
  
  {% collapsible API创建集群 %}
  
  同时，腾讯云EMR还提供了API方式构建基于Alluxio的大数据集群,具体可参考[这里](https://cloud.tencent.com/document/product/589/34261) 。
  
  {% endcollapsible %}


{% endaccordion %}

## 基础配置

创建了一个带Alluxio组件的腾讯云EMR，默认会把HDFS挂载到Alluxio上，并使用内存作为单层level0存储。如果有需要更改更符合业务特性的多级存储，或者其他对应优化项，可以使用配置下发功能来完成相关配置：
  ![](https://application-display-1259353343.cos.ap-hongkong.myqcloud.com/alluxio/alluxio-config_cn.png)
在配置下发后，有些配置需要重启Alluxio服务才能生效:
  ![](https://application-display-1259353343.cos.ap-hongkong.myqcloud.com/alluxio/alluxio-restart_cn.png)


了解跟多配置下发和重启策略细节，可以查阅相关文档:

- [配置管理](https://cloud.tencent.com/document/product/589/14628)
- [重启服务](https://cloud.tencent.com/document/product/589/32823)

## 基于Alluxio加速计算存储分离
腾讯云EMR基于腾讯云对象存储(COS)提供了计算存储分离能力，默认直接访问对象存储中的数据时，应用程序没有节点级数据本地性或跨应用程序缓存。使用 Alluxio 加速将缓解这些问题。
在腾讯云EMR集群上默认已经部署了使用COS作为UFS的依赖jar包，只需授权访问COS和把COS mount到Alluxio上即可使用。

{% accordion accelerate %}

  {% collapsible API创建集群 %}
  
若当前集群未开启对象存储，可单击 Authorize 进行授权，授权后EMR中节点可以通过临时秘钥访问COS中数据。
  ![](https://application-display-1259353343.cos.ap-hongkong.myqcloud.com/alluxio/cos-auth_cn.png)
  
  ![](https://application-display-1259353343.cos.ap-hongkong.myqcloud.com/alluxio/auth-confirm_cn.png)
  
  {% endcollapsible %}
  
{% endaccordion %}


更多在腾讯云EMR中使用Alluxio开发使用细节，可[查阅](https://cloud.tencent.com/document/product/589)
