---
布局： 全局
title： Alluxio 软件要求
---

## 一般要求

以下是本地或集群运行 Alluxio 的一般要求。

* 集群节点应运行以下支持的操作系统之一：
  * MacOS 10.10 或更高版本
  * CentOS - 6.8 或 7
  * RHEL - 7.x
  * Ubuntu - 16.04
* Java JDK 11（支持 Oracle 或 OpenJDK 发行版）
* Alluxio 仅适用于 IPv4 网络。
* 允许以下端口和协议：
  * 入站 TCP 22 - 以用户身份通过 ssh 在指定节点上安装 Alluxio 组件。
* 建议使用基于 x86 架构的硬件。经验证，Alluxio 可在 ARM 架构上运行，但某些功能可能无法运行。
   * 两种架构下的 Alluxio 整体性能相似、
  基于在[AWS Graviton 处理器](https://aws.amazon.com/ec2/graviton/)（例如[r6g 实例](https://aws.amazon.com/ec2/instance-types/r6g/)）上运行时的基准结果
  与 [AWS 第三代 AMD EPYC 处理器](https://aws.amazon.com/ec2/amd/) (ex. [r6a instances](https://aws.amazon.com/ec2/instance-types/r6a/)) 相比。

### 主要求

对于运行主进程的集群节点，Alluxio 有特定的要求。

请注意，这些只是运行软件的最低要求。
大规模、高负荷运行 Alluxio 会提高这些要求。

* 至少 1 GB 磁盘空间
* 至少 1 GB 内存（使用嵌入式日志时为 6 GB）
* 至少 2 个 CPU 内核
* 允许以下端口和协议：
  * Inbound TCP 19998 - Alluxio 主站的默认 RPC 端口
  * Inbound TCP 19999 - Alluxio 主站的默认 Web UI 端口： http://<master-hostname>:19999`。
  * Inbound TCP 20001 - Alluxio 作业主站的默认 RPC 端口
  * Inbound TCP 20002 - Alluxio 作业主站的默认 Web UI 端口
  * 仅嵌入式日志要求
    * Inbound TCP 19200 - Alluxio 主站用于内部领导选举的默认端口
    * Inbound TCP 20003 - Alluxio 作业主站用于内部领导者选举的默认端口

### 工作者要求

运行 Worker 进程的集群节点需要满足 Alluxio 的特定要求：

* 至少 1 GB 磁盘空间
* 至少 1 GB 内存
* 至少 2 个 CPU 内核
* 允许以下端口和协议：
  * Inbound TCP 29999 - Alluxio Worker 的默认 RPC 端口
  * Inbound TCP 30000 - Alluxio Worker 的默认 Web UI 端口： http://<worker-hostname>:30000
  * Inbound TCP 30001 - Alluxio 作业程序的默认 RPC 端口
  * Inbound TCP 30002 - Alluxio 作业程序的默认数据端口
  * Inbound TCP 30003 - Alluxio 作业程序的默认 Web UI 端口
    端口： http://<worker-hostname>:30003

## Fuse 要求

运行 fuse 进程的节点需要满足 Alluxio 的特定要求。

请注意，这些只是运行软件的最低要求。
在高负载情况下运行 Alluxio Fuse 会提高这些要求。

* 至少 1 个 CPU 内核
* 至少 1 GB 内存
* 已安装 FUSE
  * Linux 下 libfuse 2.9.3 或更新版本，建议使用 libfuse >= 3.0.0
  * 适用于 MacOS 的 osxfuse 3.7.1 或更新版本