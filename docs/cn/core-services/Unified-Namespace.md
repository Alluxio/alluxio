---
layout: global
title: 统一命名空间
nickname: 统一命名空间
group: Core Services
priority: 1
---

本页总结了如何在Alluxio文件系统名称空间中管理不同的底层存储系统。

* Table of Contents
{:toc}

## 介绍
Alluxio通过使用透明的命名机制和挂载API来实现有效的跨不同底层存储系统的数据管理。

### 统一命名空间

Alluxio提供的主要好处之一是为应用程序提供统一命名空间。
通过统一命名空间的抽象，应用程序可以通过统一命名空间和接口来访问多个独立的存储系统。
与其与每个独立的存储系统进行通信，应用程序可以只连接到Alluxio并委托Alluxio来与不同的底层存储通信。

![unified]({{ '/img/screenshot_unified.png' | relativize_url }})

master配置属性`alluxio.master.mount.table.root.ufs`指定的目录挂载到Alluxio命名空间根目录，该目录代表Alluxio
的"primary storage"。在此基础上，用户可以通过挂载API添加和删除数据源。

```java
void mount(AlluxioURI alluxioPath, AlluxioURI ufsPath);
void mount(AlluxioURI alluxioPath, AlluxioURI ufsPath, MountOptions options);
void unmount(AlluxioURI path);
void unmount(AlluxioURI path, UnmountOptions options);
```

例如，可以通过以下方式将一个新的S3存储桶挂载到`Data`目录中
```java
mount(new AlluxioURI("alluxio://host:port/Data"), new AlluxioURI("s3://bucket/directory"));
```

### UFS命名空间
除了Alluxio提供的统一命名空间之外，每个已挂载的基础文件系统
在Alluxio命名空间中有自己的命名空间； 称为UFS命名空间。
如果在没有通过Alluxio的情况下更改了UFS名称空间中的文件，
UFS命名空间和Alluxio命名空间可能不同步的情况。
发生这种情况时，需要执行[UFS元数据同步](＃ufs-metadata-sync)操作才能重新使两个名称空间同步。

## 透明命名机制

透明命名机制保证了Alluxio和底层存储系统命名空间身份一致性。

![transparent]({{ site.baseurl }}/img/screenshot_transparent.png)

当用户在Alluxio命名空间创建对象时，可以选择这些对象是否要在底层存储系统中持久化。对于需要持久化的对象，
Alluxio会保存底层存储系统存储这些对象的路径。例如，一个用户在根目录下创建了一个`Users`目录及`Alice`和`Bob`两个子目录，底层存储系统也会保存相同的目录结构和命名。类似地，当用户在
Alluxio命名空间中对一个持久化的对象进行重命名或者删除操作时，底层存储系统中也会对其执行相同的重命名或删除操作。

Alluxio能够透明发现底层存储系统中并非通过Alluxio创建的内容。例如，底层存储系统中包含一个`Data`文件夹，
其中包含`Reports`和`Sales`文件，都不是通过Alluxio创建的，当它们第一次被访问时，如用户请求打开文
件，Alluxio会自动加载这些对象的元数据。然而在该过程中Alluxio不会加载文件内容数据，若要将其内容加载到Alluxio，
可以用`FileInStream`来读数据，或者通过Alluxio Shell中的`load`命令。

## 挂载底层存储系统
定义Alluxio命名空间和UFS命名空间之间的关联是通过将底层存储系统挂载到Alluxio文件系统命名空间的机制完成的。
在Alluxio中挂载底层存储与在Linux文件系统中挂载一个卷类似。
mount命令将UFS挂载到Alluxio命名空间中文件系统树。

### 根挂载点
Alluxio命名空间的根挂载点是在masters上’conf/alluxio-site.properties’中配置的。
下一行是一个配置样例，一个HDFS路径挂载到
Alluxio命名空间根目录。

```
alluxio.master.mount.table.root.ufs=hdfs://HDFS_HOSTNAME:8020
```

使用配置前缀来配置根挂载点的挂载选项：

`alluxio.master.mount.table.root.option.<some alluxio property>`

例如，以下配置为根挂载点添加AWS凭证。

```
alluxio.master.mount.table.root.option.aws.accessKeyId=<AWS_ACCESS_KEY_ID>
alluxio.master.mount.table.root.option.aws.secretKey=<AWS_SECRET_ACCESS_KEY>
```

以下配置显示了如何为根挂载点设置其他参数。

```
alluxio.master.mount.table.root.option.alluxio.security.underfs.hdfs.kerberos.client.principal=client
alluxio.master.mount.table.root.option.alluxio.security.underfs.hdfs.kerberos.client.keytab.file=keytab
alluxio.master.mount.table.root.option.alluxio.security.underfs.hdfs.impersonation.enabled=true
alluxio.master.mount.table.root.option.alluxio.underfs.version=2.7
```

### 嵌套挂载点
除了根挂载点之外，其他底层文件系统也可以挂载到Alluxio命名空间中。
这些额外的挂载点可以通过`mount`命令在运行时添加到Alluxio。
`--option`选项允许用户传递挂载操作的附加参数，如凭证。

```console
# the following command mounts an hdfs path to the Alluxio path `/mnt/hdfs`
$ ./bin/alluxio fs mount /mnt/hdfs hdfs://host1:9000/data/
# the following command mounts an s3 path to the Alluxio path `/mnt/s3` with additional options specifying the credentials
$ ./bin/alluxio fs mount \
  --option aws.accessKeyId=<accessKeyId> --option aws.secretKey=<secretKey> \
  /mnt/s3 s3://data-bucket/
```

注意，挂载点也允许嵌套。 例如，如果将UFS挂载到
`alluxio:///path1`，可以在`alluxio:///path1/path2`处挂载另一个UFS。

### 使用特定版本挂载UFS

Alluxio支持挂载特定不同版本HDFS。
因此，用户可以将不同版本的HDFS挂载到同一个Alluxio命名空间中。 有关更多详细信息，请参考[HDFS底层存储]({{ '/en/ufs/HDFS.html' | relativize_url}})。

## Alluxio和UFS命名空间之间的关系
Alluxio提供了一个统一的命名空间，充当一个或多个底层文件存储系统的数据缓存层。 本节讨论Alluxio如何与底层文件系统交互来发现和通过Alluxio呈现这些文件。

通过Alluxio访问UFS文件的与直接通过UFS访问文件的相同。
如果UFS根目录是`s3://bucket/data`，则列出`alluxio:///`下内容应该与列出`s3://bucket/data`相同。
在`alluxio:///file`上运行cat的结果应与在`s3://bucket/data/file`上运行cat的结果相同。

Alluxio按需从UFS加载元数据。
在上面的示例中，Alluxio在启动时并没有有关`s3://bucket/data/file`的信息。
直到当用户试图列出`alluxio:///`或尝试使用cat `alluxio:///file`时，才发现该文件。
这样好处是可以防止在安装新的UFS时进行不必要的文件发现工作。

默认情况下，* Alluxio预期所有对底层文件系统修改都是通过Alluxio 来进行的*。
这样Alluxio只需扫描每个UFS目录一次，从而在UFS元数据操作很慢情况下显著提高性能。
当出现在Alluxio之外对UFS进行更改的情况下，
就需要用元数据同步功能用于同步两个命名空间。

### UFS元数据同步

> UFS元数据同步功能新增自版本`1.7.0`。

当Alluxio扫描UFS目录并加载其子目录元数据时，
它将创建元数据的副本，以便将来无需再从UFS加载。
元数据的缓存副本将根据
`alluxio.user.file.metadata.sync.interval`客户端属性配置的间隔段刷新。
此属性适用于客户端操作。
例如，如果客户执行一个命令基于间隔设置为一分钟的配置，
如果最后一次刷新是在一分钟之前，则相关元数据将据UFS刷新。
设值为`0`表示针对每个操作都会进行实时元数据同步，
而默认值`-1`表示在初始加载后不会再重新同步元数据。

低间隔值使Alluxio客户端可以快速发现对UFS的外部修改，
但由于导致调用UFS的次数增加，因此是以降低性能为代价的。

元数据同步会保留每个UFS文件的指纹记录，以便Alluxio可以在文件更改时做出相应更新。
指纹记录包括诸如文件大小和上次修改时间之类的信息。
如果在UFS中修改了文件，Alluxio将通过指纹检测到该修改，释放现有文件
元数据，然后重新加载更新文件的元数据。
如果在UFS中添加或删除了文件，Alluxio还将更新对其命名空间中的元数据做出相应刷新。

### 用于管理UFS同步的方法

#### 定期元数据同步

如果UFS按计划的间隔更新，可以在更新后手动触发sync命令。
运行以下命令将同步间隔设置为`0`：

```console
$ ./bin/alluxio fs ls -R -Dalluxio.user.file.metadata.sync.interval=0 /path/to/sync
```
#### 集中配置

对于使用来自频繁更新的UFS数据的集群作业，
每个客户端指定一个同步间隔很不方便。
如果在master配置中设置了同步间隔，所有请求都将以默认的同步间隔来处理。

在master点上的`alluxio-site.properties`中设置：

`alluxio.user.file.metadata.sync.interval=1m`

注意，需要重新启动master节点以便启用新配置。

###其他加载新UFS文件的方法

建议使用前面讨论的UFS同步的方法来同步UFS中的更改。
这是是其他一些加载文件的方法：

*`alluxio.user.file.metadata.load.type`：此客户端属性可以设置为
`ALWAYS`，`ONCE`或`NEVER`。此属性类似`alluxio.user.file.metadata.sync.interval`，
但有注意事项：
    1.它只会发现新文件，不会重新加载修改或删除的文件。
    1.它仅适用于`exists`，`list`和`getStatus` RPC。
    
    `ALWAYS`配置意味者总会检查UFS中是否有新文件，`ONCE`将使用默认值
仅扫描每个目录一次，而`NEVER`配置下Alluxio根本不会
扫描新文件。

*`alluxio fs ls -f /path`:`ls`的`-f`选项相当于设置
`alluxio.user.file.metadata.load.type`为`ALWAYS`。它将发现新文件，但
不检测修改或删除的UFS文件。
要检测修改或删除的UFS文件的唯一方法是通过传递
`-Dalluxio.user.file.metadata.sync.interval=0`选项给`ls`。

## 示例

以下示例假设Alluxio源代码在`${ALLUXIO_HOME}`文件夹下，并且有一个本地运行的Alluxio进程。

### 透明命名

先在本地文件系统中创建一个将作为底层存储挂载的临时目录：

```console
$ cd /tmp
$ mkdir alluxio-demo
$ touch alluxio-demo/hello
```
将创建的目录挂载到Alluxio命名空间中，并确认挂载后的目录在Alluxio中存在：

```console
$ cd ${ALLUXIO_HOME}
$ ./bin/alluxio fs mount /demo file:///tmp/alluxio-demo
Mounted file:///tmp/alluxio-demo at /demo
$ ./bin/alluxio fs ls -R /
... # note that the output should show /demo but not /demo/hello
```

验证对于不是通过Alluxio创建的内容，当第一次被访问时，其元数据被加载进入了Alluxio中:

```console
$ ./bin/alluxio fs ls /demo/hello
... # should contain /demo/hello
```

在挂载目录下创建一个文件，并确认在底层文件系统中该文件也被以同样名字创建了：

```console
$ ./bin/alluxio fs touch /demo/hello2
/demo/hello2 has been created
$ ls /tmp/alluxio-demo
hello hello2
```
在Alluxio中重命名一个文件，并验证在底层文件系统中该文件也被重命名了：

```console
$ ./bin/alluxio fs mv /demo/hello2 /demo/world
Renamed /demo/hello2 to /demo/world
$ ls /tmp/alluxio-demo
hello world
```
在Alluxio中删除一个文件，然后确认该文件是否在底层文件系统中也被删除了：

```console
$ ./bin/alluxio fs rm /demo/world
/demo/world has been removed
$ ls /tmp/alluxio-demo
hello
```
卸载该挂载目录，并确认该目录已经在Alluxio命名空间中被删除，但该目录依然保存在底层文件系统中。

```console
$ ./bin/alluxio fs unmount /demo
Unmounted /demo
$ ./bin/alluxio fs ls -R /
... # should not contain /demo
$ ls /tmp/alluxio-demo
hello
```

### HDFS元数据主动同步
在2.0版中，引入了一项新功能，用于在UFS为HDFS时保持Alluxio空间与UFS之间的同步。
该功能称为主动同步，可监听HDFS事件并以master上后台任务方式定期在UFS和Alluxio命名空间之间同步元数据。
由于主动同步功能取决于HDFS事件，因此仅当UFS HDFS版本高于2.6.1时，此功能才可用。
你可能需要在配置文件中更改`alluxio.underfs.version`的值。
有关所支持的Hdfs版本的列表，请参考[HDFS底层存储]({{ '/en/ufs/HDFS.html#supported-hdfs-versions' | relativize_url}})。

要在一个目录上启用主动同步，运行以下Alluxio命令。

```console
$ ./bin/alluxio fs startSync /syncdir
```

可以通过更改`alluxio.master.ufs.active.sync.interval`选项来控制主动同步间隔，默认值为30秒。

要在一个目录上停止使用主动同步，运行以下Alluxio命令。

```console
$ ./bin/alluxio fs stopSync /syncdir
```

> 注意：发布`startSync`时，就预定了对同步点进行完整扫描。
> 如果以Alluxio超级用户身份运行，`stopSync`将中断所有尚未结束的完整扫描。
> 如果以其他用户身份运行，`stopSync`将等待完整扫描完成后再执行。

可以使用以下命令检查哪些目录当前处于主动同步状态。

```console
$ ./bin/alluxio fs getSyncPathList
```

#### 主动同步的静默期

主动同步会尝试避免在目标目录被频繁使用时进行同步。
它会试图在UFS活动期寻找一个静默期，再开始UFS和Alluxio空间之间同步，以避免UFS繁忙时使其过载。
有两个配置选项来控制此特性。

`alluxio.master.ufs.active.sync.max.activities`是UFS目录中的最大活动数。
活动数的计算是基于目录中事件数的指数移动平均值的启发式方法。
例如，如果目录在过去三个时间间隔中有100、10、1个事件。
它的活动为`100/10 * 10 + 10/10 + 1 = 3`
`alluxio.master.ufs.active.sync.max.age`是在同步UFS和Alluxio空间之前将等待的最大间隔数。

系统保证如果目录“静默”或长时间未同步(超过最大期限)，我们将开始同步该目录。

例如，以下设置

```
alluxio.master.ufs.active.sync.interval=30sec
alluxio.master.ufs.active.sync.max.activities=100
alluxio.master.ufs.active.sync.max.age=5
```

表示系统每隔30秒就会计算一次此目录的事件数，
并计算其活动。
如果活动数少于100，则将其视为一个静默期，并开始同步
该目录。
如果活动数大于100，并且在最近5个时间间隔内未同步，或者
5 * 30 = 150秒，它将开始同步目录。
如果活动数大于100并且至少已在最近5个间隔中同步过一次，将不会执行主动同步。

### 统一命名空间

此示例将安装多个不同类型的底层存储，以展示统一文件系统命名空间的抽象作用。 本示例将使用属于不同AWS账户和一个HDSF服务的两个S3存储桶。

使用相对应凭证`<accessKeyId1>`和`<secretKey1>`将第一个S3存储桶挂载到Alluxio中：

```console
$ ./bin/alluxio fs mkdir /mnt
$ ./bin/alluxio fs mount \
  --option aws.accessKeyId=<accessKeyId1> \
  --option aws.secretKey=<secretKey1> \
  /mnt/s3bucket1 s3://data-bucket1/
```

使用相对应凭证’<accessKeyId2>’和’ <secretKey2>’将第二个S3存储桶挂载到Alluxio：

```console
$ ./bin/alluxio fs mount \
  --option aws.accessKeyId=<accessKeyId2> \
  --option aws.secretKey=<secretKey2> \
  /mnt/s3bucket2 s3://data-bucket2/
```

将HDFS存储挂载到Alluxio：

```console
$ ./bin/alluxio fs mount /mnt/hdfs hdfs://<NAMENODE>:<PORT>/
```

所有这三个目录都包含在Alluxio的一个命名空间中：

```console
$ ./bin/alluxio fs ls -R /
... # should contain /mnt/s3bucket1, /mnt/s3bucket2, /mnt/hdfs
```

## 资源

- 一篇博客文章，解释了[统一命名空间](https://www.alluxio.io/resources/whitepapers/unified-namespace-allowing-applications-to-access-data-anywhere/)
- 关于[优化以加快元数据操作速度]的博客文章(https://www.alluxio.io/blog/how-to-speed-up-alluxio-metadata-operations-up-to-100x/)
