---
布局: 全局
标题: Presto使用Alluxio带有本地缓存的SDK
---

Presto可以通过Alluxio SDK与Alluxio集成。
通过使用SDK，需要经常扫描的热数据可以被缓存到执行TableScan operator的本地Presto worker节点上。

## 前期准备：
- 安装Java 8 Update 161版本或更高版本(8u161+)的64位Java。
- [部署 Presto](https://prestodb.io/docs/current/installation/deployment.html){:target="_blank"}.
- Alluxio已经按照[此处的部署指南]({{ '/en/deploy/Install-Alluxio-Cluster-with-HA.html' | relativize_url }})设置并运行 。
- 确保提供SDK的Alluxio客户端jar包是可用的。在从Alluxio下载页面下载的压缩包的 `/<PATH_TO_ALLUXIO>/client/alluxio-${VERSION}-client.jar`中可以找到Alluxio客户端jar包。
- 确保Hive Metastore正在运行以提供 Hive 表的元数据信息。Hive Metastore的默认端口是`9083`。执行 `lsof -i:9083` 可查看 Hive Metastore 进程是否存在。

## 基础设置
### 配置Presto连接到Hive Metastore
Presto通过 Presto 的Hive连接器从 Hive Metastore中获取数据库和表元数据的信息（包括表数据的文件系统位置）。
以下是catalog使用Hive连接器，并且Metastore服务位于`localhost`情况下的Presto示例配置文件`${PRESTO_HOME}/etc/hive.properties`。
```properties
connector.name=hive-hadoop2
hive.metastore.uri=thrift://localhost:9083
```

### 启用 Presto 的本地缓存
要启用本地缓存，在${PRESTO_HOME}/etc/hive.properties中添加以下配置：
```properties
hive.node-selection-strategy=SOFT_AFFINITY
cache.enabled=true
cache.type=ALLUXIO
cache.base-directory=file:///tmp/alluxio
cache.alluxio.max-cache-size=100MB
```
这里的`cache.enabled=true`和`cache.type=ALLUXIO`用于启用Presto中的本地缓存功能。cache.base-directory用于指定本地缓存的路径。`cache.alluxio.max-cache-size`用于分配本地缓存空间。

### 将Alluxio客户端jar包分发给所有Presto服务器
由于Presto通过Alluxio客户端jar包提供的SDK与Alluxio服务器通信，Alluxio客户端jar包必须在Presto服务器的类路径中。将Alluxio客户端jar包/<PATH_TO_ALLUXIO>/client/alluxio-2.9.1-client.jar放入所有Presto服务器的${PRESTO_HOME}/plugin/hive-hadoop2/目录(directory)中(该目录可能因版本而异)。
重启Presto worker 和 coordinator
```shell
$ ${PRESTO_HOME}/bin/launcher restart
```
在完成基础配置后，Presto应该能够访问Alluxio中的数据。

## 示例
### 创建Hive表
通过Hive客户端创建一个Hive表，指定其`LOCATION`为Alluxio。
```sql
hive> CREATE TABlE employee_parquet_alluxio (name string, salary int)
PARTITIONED BY (doj string)
STORED AS PARQUET
LOCATION 'alluxio://Master01:19998/alluxio/employee_parquet_alluxio';
```
将`Master01:19998`替换为您的Alluxio master 节点地址。请注意，我们在此处设置`STORED AS PARQUET`，因为目前Presto本地缓存只支持parquet和orc格式。

### 插入数据
在创建的Hive表中插入一些数据进行测试。
```sql
INSERT INTO employee_parquet_alluxio select 'jack', 15000, '2023-02-26';
INSERT INTO employee_parquet_alluxio select 'make', 25000, '2023-02-25';
INSERT INTO employee_parquet_alluxio select 'amy', 20000, '2023-02-26';
```
### 使用Presto查询表
按照 [Presto CLI指南](https://prestodb.io/docs/current/installation/cli.html){:target="_blank"} 下载`presto-cli-<PRESTO_VERSION>-executable.jar`， 将其重命名为`presto-cli`，并使用`chmod + x`使其可执行。使用`presto-cli`运行单个查询, 从表中选取数据。
```sql
presto> SELECT * FROM employee_parquet_alluxio;
```
您可以看到数据被缓存在`/etc/catalog/hive.properties`指定的directory中。
在我们的示例中，应该会看到文件被缓存在`/tmp/alluxio/LOCAL`中。

## 高级设置
### 监控有关本地缓存的指标
要暴露本地缓存指标，请按照以下步骤进行操作：
- **步骤1**：添加`-Dalluxio.metrics.conf.file=<ALLUXIO_HOME>/conf/metrics.properties`以指定Presto使用的SDK的指标配置。
- **步骤2**：在`<ALLUXIO_HOME>/conf/metrics.properties`中添加`sink.jmx.class=alluxio.metrics.sink.JmxSink`以暴露指标。 
- **步骤3**：在`<PRESTO_HOME>/etc/catalog.hive.properties`中添加`cache.alluxio.metrics-enabled=true`以启用指标收集。
- **步骤4**：执行`<PRESTO_HOME>/bin/launcher restart`重启Presto进程。
- **步骤5**：如果访问Presto的JMX RESTful API `<PRESTO_NODE_HOST_NAME>:<PRESTO_PORT>/v1/jmx`，应该可以看到有关本地缓存的指标。 

以下指标对于追踪本地缓存非常有用：
<table class="table table-striped">
    <tr>
        <th>指标名称</th>
        <th>类型</th>
        <th>描述</th>
    </tr>
    <tr>
        <td markdown="span">`Client.CacheBytesReadCache`</td>
        <td>计量器</td>
        <td>从客户端读取的字节数。</td>
    </tr>
    <tr>
        <td markdown="span">`Client.CachePutErrors`</td>
        <td>计数器</td>
        <td>将缓存数据放入客户端缓存时失败的次数。</td>
    </tr>
    <tr>
        <td markdown="span">`Client.CachePutInsufficientSpaceErrors`</td>
        <td>计数器</td>
        <td>由于驱逐数据后的空间不足，将缓存数据放入客户端缓存时出现失败的次数。</td>
    </tr>
    <tr>
        <td markdown="span">`Client.CachePutNotReadyErrors`</td>
        <td>计数器</td>
        <td>当缓存尚未准备好添加页面时出现失败的次数。</td>
    </tr>
    <tr>
        <td markdown="span">`Client.CachePutBenignRacingErrors`</td>
        <td>计数器</td>
        <td>由于竞争驱逐而导致添加页面时出现失败的次数。此错误是无害的。</td>
    </tr>
    <tr>
        <td markdown="span">`Client.CachePutStoreWriteErrors`</td>
        <td>计数器</td>
        <td>由于向页面存储写入失败，将缓存数据放入客户端缓存时出现失败的次数。</td>
    </tr>
    <tr>
        <td markdown="span">`Client.CachePutEvictionErrors`</td>
        <td>计数器</td>
        <td>由于驱逐失败，将缓存数据放入客户端缓存时出现失败的次数。</td>
    </tr>
    <tr>
        <td markdown="span">`Client.CachePutStoreDeleteErrors`</td>
        <td>计数器</td>
        <td>由于页面存储中的删除失败，将缓存数据放入客户端缓存时出现失败的次数。</td>
    </tr>
    <tr>
        <td markdown="span">`Client.CacheGetErrors`</td>
        <td>计数器</td>
        <td>从客户端缓存中获取缓存数据时出现失败的次数。</td>
    </tr>
    <tr>
        <td markdown="span">`Client.CacheGetNotReadyErrors`</td>
        <td>计数器</td>
        <td>当缓存尚未准备好获取页面时出现失败的次数。</td>
    </tr>
    <tr>
        <td markdown="span">`Client.CacheGetStoreReadErrors`</td>
        <td>计数器</td>
        <td>由于从页面存储中读取失败，从客户端缓存中获取缓存数据时出现失败的次数。</td>
    </tr>
    <tr>
        <td markdown="span">`Client.CacheDeleteNonExistingPageErrors`</td>
        <td>计数器</td>
        <td>由于页面缺失而导致删除页面时出现失败的次数。</td>
    </tr>
    <tr>
        <td markdown="span">`Client.CacheDeleteNotReadyErrors`</td>
        <td>计数器</td>
        <td>当缓存尚未准备好删除页面时出现失败的次数。</td>
    </tr>
    <tr>
        <td markdown="span">`Client.CacheDeleteFromStoreErrors`</td>
        <td>计数器</td>
        <td>从页面存储中删除页面时出现失败的次数。</td>
    </tr>
    <tr>
        <td markdown="span">`Client.CacheHitRate`</td>
        <td>仪表</td>
        <td>缓存命中率：（从缓存中读取的字节数）/（请求的字节数）。</td>
    </tr>
    <tr>
        <td markdown="span">`Client.CachePagesEvicted`</td>
        <td>计量器</td>
        <td>从客户端缓存中驱逐的页面总数。</td>
    </tr>
    <tr>
        <td markdown="span">`Client.CacheBytesEvicted`</td>
        <td>计量器</td>
        <td>从客户端缓存中驱逐的字节总数。</td>
    </tr>
</table>
