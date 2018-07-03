---
layout: global
title: 安全性
nickname: 安全性
group: Features
priority: 1
---

* 内容列表
{:toc}

该文档介绍Alluxio安全性的概念以及用法。

1. [安全认证](#authentication): 如果是`alluxio.security.authentication.type=SIMPLE`(默认情况下),
Alluxio文件系统识别访问服务的用户。使用其他安全特性(如授权)需要具有`SIMPLE`身份验证。
还支持其他身份验证模式，如`NOSASL`和`CUSTOM`。
2. [访问权限控制](#authorization): 如果是 `alluxio.security.authorization.permission.enabled=true`
(在默认情况下)，根据请求用户和要访问的文件或目录的POSIX权限模型，Alluxio文件系统将授予或拒绝用户访问。
注意，身份验证不能是“NOSASL”，因为授权需要用户信息。
3. [审查机制](#auditing): 如果是 `alluxio.master.audit.logging.enabled=true`, Alluxio 文件系统
维护用户访问文件元数据的审计日志。

参考[安全性配置项](Configuration-Settings.html#security-configuration)的信息以启用不同安全特性。

## 安全认证 {#Authentication}

### SIMPLE

当`alluxio.security.authentication.type` 类型'被设置为`SIMPLE`，身份验证被启用。
在客户端访问服务之前，该客户端按以下列次序指示Alluxio服务检索要报告的用户信息

1. 如果`alluxio.security.login.username` 在客户端上设置，其值将作为
此客户端的登录用户。

2. 否则，将从操作系统获取登录用户。

客户端检索用户信息后，将使用该用户信息进行连接该服务。在客户端创建目录/文件之后，将用户信息添加到元数据中

并且可以在CLI和UI中检索。

### NOSASL

当`alluxio.security.authentication.type` 值为 `NOSASL`,，身份验证被禁用。服务将忽略客户端的用户，
并且没有任何信息将与该用户创建的文件或目录相关联。

### CUSTOM

当`alluxio.security.authentication.type` 值为 `CUSTOM`，身份验证被启用。Alluxio客户端
检查 `alluxio.security.authentication.custom.provider.class`类的名称
用于检索用户。此类是`alluxio.security.authentication.AuthenticationProvider`的实现

这种模式目前还处于试验阶段，应该只在测试中使用。

## 访问权限控制

Alluxio文件系统为目录和文件实现了一个访问权限模型，该模型与POSIX标准的访问权限模型类似。

每个文件和目录都与以下各项相关联：

1. 一个所属用户，即在client进程中创建该文件或文件夹的用户。
2. 一个所属组，即从用户-组映射（user-groups-mapping）服务中获取到的组。参考[用户-组映射](#user-group-mapping)。
3. 访问权限。

访问权限包含以下三个部分：

1. 所属用户权限，即该文件的所有者的访问权限
2. 所属组权限，即该文件的所属组的访问权限
3. 其他用户权限，即上述用户之外的其他所有用户的访问权限

每项权限有三种行为：

1. read (r)
2. write (w)
3. execute (x)

对于文件，读取该文件需要r权限，修改该文件需要w权限。对于目录，列出该目录的内容需要r权限，在该目录下创建、重命名或者删除其子文件或子目录需要w权限，访问该目录下的子项需要x权限。

例如，当启用访问权限控制时，运行shell命令`ls -R`后其输出如下：

```bash
$ ./bin/alluxio fs ls /
drwxr-xr-x jack           staff                       24       PERSISTED 11-20-2017 13:24:15:649  DIR /default_tests_files
-rw-r--r-- jack           staff                       80   NOT_PERSISTED 11-20-2017 13:24:15:649 100% /default_tests_files/BASIC_CACHE_PROMOTE_MUST_CACHE
```

### 用户-组映射 

当用户确定后，其组列表通过一个组映射服务确定，该服务通过`alluxio.security.group.mapping.class`配置，其默认实现是
`alluxio.security.group.provider.ShellBasedUnixGroupsMapping`，该实现通过执行`groups` shell命令获取一个给定用户的组关系。
用户-组映射默认使用了一种缓存机制，映射关系默认会缓存60秒，这个可以通过`alluxio.security.group.mapping.cache.timeout.ms`进行配置，如果这个值设置成为“0”，缓存就不会启用.

`alluxio.security.authorization.permission.supergroup`属性定义了一个超级组，该组中的所有用户都是超级用户。

### 目录和文件初始访问权限

初始创建访问权限是777,并且目录和文件的区别为111。默认的umask值为022，新创建的目录权限为755，文件为644。umask可以通过`alluxio.security.authorization.permission.umask`属性设置。

### 更新目录和文件访问权限

所属用户、所属组以及访问权限可以通过以下两种方式进行修改：

1. 用户应用可以调用`FileSystem API`或`Hadoop API`的setAttribute(...)方法，参考[文件系统API](File-System-API.html)。
2. CLI命令，参考[chown, chgrp, chmod](Command-Line-Interface.html#list-of-operations)。

所属用户只能由超级用户修改。
所属组和访问权限只能由超级用户和文件所有者修改。

## 模拟 {Impersonation}
Alluxio支持用户模拟，以便用户代表另一个用户访问Alluxio。如果Alluxio客户端是一个为多个用户提供数据访问的服务的一部分时，这个机制会相当有用。
在这种情况下，可以将Alluxio客户端配置为用特定用户（连接用户）连接到Alluxio服务器，但代表其他用户（模拟用户）行事。
为了让Alluxio支持用户模拟功能，需要在客户端和服务端进行配置。

### 服务端配置
为了能够让特定的用户模拟其他用户，需要配置Alluxio master。master的配置属性包括：`alluxio.master.security.impersonation.<USERNAME>.users` 和 `alluxio.master.security.impersonation.<USERNAME>.groups`。
对于`alluxio.master.security.impersonation.<USERNAME>.users`，你可以指定由逗号分隔的用户列表，这些用户可以被`<USERNAME>` 模拟。
通配符`*`表示任意的用户可以被`<USERNAME>` 模拟。以下是例子。
- `alluxio.master.security.impersonation.alluxio_user.users=user1,user2`
    - 这意味着Alluxio用户`alluxio_user`可以模拟用户`user1`以及`user2`。
- `alluxio.master.security.impersonation.client.users=*`
    - 这意味着Alluxio用户`client`可以模拟任意的用户。

对于`alluxio.master.security.impersonation.<USERNAME>.groups`，你可以指定由逗号分隔的用户组，这些用户组内的用户可以被`<USERNAME>`模拟。
通配符`*`表示该用户可以模拟任意的用户。以下是一些例子。
- `alluxio.master.security.impersonation.alluxio_user.groups=group1,group2`
    - 这意味着Alluxio用户`alluxio_user`可以模拟用户`group1`以及`group2`中的任意用户。
- `alluxio.master.security.impersonation.client.groups=*`
    - 这意味着Alluxio用户`client`可以模拟任意的用户。

为了使得用户`alluxio_user`能够模拟其他用户，你至少需要设置`alluxio.master.security.impersonation.<USERNAME>.users`和
`alluxio.master.security.impersonation.<USERNAME>.groups`的其中一个（将`<USERNAME>`替换为`alluxio_user`）。你可以将两个参数设置为同一个用户。

### 客户端配置
如果master配置为允许某些用户模拟其他的用户，client端也要进行相应的配置。使用`alluxio.security.login.impersonation.username`进行配置。
这表示进行连接的Alluxio client保持不变，但是是模拟的一个不同的用户。参数可以设置为以下值：

empty
不使用Alluxio client模拟
`_NONE_`
不使用Alluxio client模拟
`_HDFS_USER_`
Alluxio client会模拟HDFS client的同一用户（当使用Hadoop兼容的client时）

## 审查
Alluxio支持审查日志用于系统管理员追踪用户对文件元数据的访问操作。

审查日志文件(`master_audit.log`) 包括多个审查记录条目，每个条目对应一次文件元数据获取记录。
Alluxio审查日志格式如下表所示：

<table class="table table-striped">
<tr><th>key</th><th>value</th></tr>
<tr>
  <td>succeeded</td>
  <td>如果命令成功运行，值为true。在命令成功运行前，该命令必须是被允许的。 </td>
</tr>
<tr>
  <td>allowed</td>
  <td>如果命令是被允许的，值为true。即使一条命令是被允许的它也可能运行失败。 </td>
</tr>
<tr>
  <td>ugi</td>
  <td>用户组信息，包括用户名，主要组，认证类型。 </td>
</tr>
<tr>
  <td>ip</td>
  <td>客户端IP地址。 </td>
</tr>
<tr>
  <td>cmd</td>
  <td>用户运行的命令。 </td>
</tr>
<tr>
  <td>src</td>
  <td>源文件或目录地址。 </td>
</tr>
<tr>
  <td>dst</td>
  <td>目标文件或目录的地址。如果不适用，值为空。 </td>
</tr>
<tr>
  <td>perm</td>
  <td>user:group:mask，如果不适用值为空。 </td>
</tr>
</table>

它和HDfS审查日志的格式很像，参考[wiki](https://wiki.apache.org/hadoop/HowToConfigure)。

要使用Alluxio审查记录功能，你需要将JVM参数`alluxio.master.audit.logging.enabled`设置为true，具体可见[Configuration settings](Configuration-Settings.html)。

## 加密

目前，服务层的加解密方案还没有完成，但是用户可以在应用层对敏感数据进行加密，或者是开启底层系统的加密功能，比如，HDFS的透明加解密，Linux的磁盘加密。

## 部署

推荐由同一个用户启动Alluxio master和workers。Alluxio集群服务包括master和workers，每个worker需要通过RPC与master通信以进行某些文件操作。如果一个worker的用户与master的不一致，这些文件操作可能会由于权限检查而失败。
