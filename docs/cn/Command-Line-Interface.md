---
layout: global
title: 命令行接口
group: Features
priority: 0
---

Alluxio命令行接口为用户提供了基本的文件系统操作，可以使用以下语法调用命令行功能：

{% include Command-Line-Interface/alluxio-fs.md %}

fs命令中的所有“路径”都应该以以下开头：

{% include Command-Line-Interface/alluxio-path.md %}

或者如果没有提供路径前缀，默认的主机名和端口（见env文件）将会被使用。

    /<path>

## 通配符输入

大多数需要路径参数的命令可以使用通配符以便简化使用，例如：

{% include Command-Line-Interface/rm.md %}

该示例命令会将`data`文件夹下以`2014`为文件名前缀的所有文件删除。

注意有些shell会尝试自动补全输入路径，从而引起奇怪的错误（注意：以下例子中的数字可能不是21，这取决于你的本地文件系统中匹配文件的个数）：

{% include Command-Line-Interface/rm-error.md %}

作为一种绕开这个问题的方式，你可以禁用自动补全功能（跟具体shell有关，例如`set -f`），或者使用转义通配符，例如：

{% include Command-Line-Interface/escape.md %}

注意是两个转义符号，这是因为该shell脚本最终会调用一个java程序运行，该java程序将获取到转义输入参数（cat /\\*）。

# 操作列表

<table class="table table-striped">
  <tr><th>操作</th><th>语法</th><th>描述</th></tr>
  {% for item in site.data.table.operation-command %}
    <tr>
      <td>{{ item.operation }}</td>
      <td>{{ item.syntax }}</td>
      <td>{{ site.data.table.cn.operation-command[item][operation] }}</td>
    </tr>
  {% endfor %}
</table>

# 使用示例

## cat

`cat`命令将Alluxio中的一个文件内容全部打印在控制台中，这在用户确认一个文件的内容是否和预想的一致时非常有用。如果你想将文件拷贝到本地文件系统中，使用`copyToLocal`命令。

例如，当测试一个新的计算任务时，`cat`命令可以用来快速确认其输出结果：

{% include Command-Line-Interface/cat.md %}

## checkConsistency

`checkConsistency`命令会对比一给定路径下Alluxio以及底层存储系统的元数据，如果该路径是一个目录，那么其所有子内容都会被对比。该命令返回包含所有不一致的文件和目录的列表，系统管理员决定是否对这些不一致数据进行调整。为了防止Alluxio与底层存储系统的元数据不一致，应将你的系统设置为通过Alluxio来修改文件和目录，而不是直接访问底层存储系统进行修改。

注意：该命令需要请求将要被检查的目录子树的读锁，这意味着在该命令完成之前无法对该目录子树的文件或者目录进行写操作或者更新操作。

例如，`checkConsistency`命令可以用来周期性地检查命名空间的完整性：

{% include Command-Line-Interface/checkConsistency.md %}

## checksum

`checksum`命令输出某个Alluxio文件的md5值。

例如，`checksum`可以用来验证Alluxio中的文件内容与存储在底层文件系统或者本地文件系统中的文件内容是否匹配：

{% include Command-Line-Interface/checksum.md %}

## chgrp

`chgrp`命令可以改变Alluxio中的文件或文件夹的所属组，Alluxio支持POSIX标准的文件权限，组在POSIX文件权限模型中是一个授权实体，文件所有者或者超级用户可以执行这条命令从而改变一个文件或文件夹的所属组。

加上`-R`选项可以递归的改变文件夹中子文件和子文件夹的所属组。

使用举例：使用`chgrp`命令能够快速修改一个文件的所属组：

{% include Command-Line-Interface/chgrp.md %}

## chmod

`chmod`命令修改Alluxio中文件或文件夹的访问权限，目前可支持八进制模式：三位八进制的数字分别对应于文件所有者、所属组以及其他用户的权限。以下是数字与权限的对应表：

<table class="table table-striped">
  <tr><th>Number</th><th>Permission</th><th>rwx</th></tr>
  {% for item in site.data.table.chmod-permission %}
    <tr>
      <td>{{ item.number }}</td>
      <td>{{ item.permission }}</td>
      <td>{{ item.rwx }}</td>
    </tr>
  {% endfor %}
</table>

加上`-R`选项可以递归的改变文件夹中子文件和子文件夹的权限。

使用举例：使用`chmod`命令可以快速修改一个文件的权限：

{% include Command-Line-Interface/chmod.md %}

## chown

`chown`命令用于修改Alluxio中文件或文件夹的所有者，出于安全方面的考虑，只有超级用户能够更改一个文件的所有者。

加上`-R`选项可以递归的改变文件夹中子文件和子文件夹的所有者。

使用举例：使用`chown`命令可以快速修改一个文件的所有者。

{% include Command-Line-Interface/chown.md %}

## copyFromLocal

`copyFromLocal`命令将本地文件系统中的文件拷贝到Alluxio中，如果你运行该命令的机器上有Alluxio worker，那么数据便会存放在这个worker上，否则，数据将会随机地复制到一个运行Alluxio worker的远程节点上。如果该命令指定的目标是一个文件夹，那么这个文件夹及其所有内容都会被递归复制到Alluxio中。

使用举例：使用`copyFromLocal`命令可以快速将数据复制到alluxio系统中以便后续处理：

{% include Command-Line-Interface/copyFromLocal.md %}

## copyToLocal

`copyToLocal`命令将Alluxio中的文件复制到本地文件系统中，如果该命令指定的目标是一个文件夹，那么该文件夹及其所有内容都会被递归地复制。

使用举例：使用`copyToLocal`命令可以快速将输出数据下载下来从而进行后续研究或调试：

{% include Command-Line-Interface/copyToLocal.md %}

## count

`count`命令输出Alluxio中所有名称匹配一个给定前缀的文件及文件夹的总数，以及它们总的大小，该命令对文件夹中的内容递归处理。当用户对文件有预定义命名习惯时，`count`命令很有用。

使用举例：若文件是以它们的创建日期命名，使用`count`命令可以获取任何日期、月份以及年份的所有文件的数目以及它们的总大小：

{% include Command-Line-Interface/count.md %}

## cp

`cp`命令拷贝Alluxio文件系统中的一个文件或者目录。

如果使用了`-R`选项，并且源路径是一个目录，`cp`将源路径下的整个子树拷贝到目标路径。

例如，`cp`可以在底层文件系统之间拷贝文件。

{% include Command-Line-Interface/cp.md %}

## du

`du`命令输出一个文件的大小，如果指定的目标为文件夹，该命令输出该文件夹下所有子文件及子文件夹中内容的大小总和。

使用举例：如果Alluxio空间被过分使用，使用`du`命令可以检测到哪些文件夹占用了大部分空间：

{% include Command-Line-Interface/du.md %}

## fileInfo

`fileInfo`命令将一个文件的主要文件信息输出到控制台，这主要是为了让用户调试他们的系统。一般来说，在Web UI上查看文件信息要容易理解得多。

使用举例：使用`fileInfo`命令能够获取到一个文件的数据块的位置，这在获取计算任务中的数据局部性时非常有用。

{% include Command-Line-Interface/fileInfo.md %}

## free

`free`命令请求Alluxio master将一个文件的所有数据块从Alluxio worker中剔除，如果命令参数为一个文件夹，那么会递归作用于其子文件和子文件夹。该请求不保证会立即产生效果，因为该文件的数据块可能正在被读取。`free`命令在被master接收后会立即返回。注意该命令不会删除底层文件系统中的任何数据，而只会影响存储在Alluxio中的数据。另外，该操作也不会影响元数据，这意味着如果运行`ls`命令，该文件仍然会被显示。

使用举例：使用`free`命令可以手动管理Alluxio的数据缓存。

{% include Command-Line-Interface/free.md %}

## getCapacityBytes

`getCapacityBytes`命令返回Alluxio被配置的最大字节数容量。

使用举例：使用`getCapacityBytes`命令能够确认你的系统是否正确启动。

{% include Command-Line-Interface/getCapacityBytes.md %}

## getUsedBytes

`getUsedBytes`命令返回Alluxio中以及使用的空间字节数。

使用举例：使用`getUsedBytes`命令能够监控集群健康状态。

{% include Command-Line-Interface/getUsedBytes.md %}

## leader

`leader`命令打印当前Alluxio的leader master节点主机名。

{% include Command-Line-Interface/leader.md %}

## load

`load` 命令将底层文件系统中的数据载入到Alluxio中。如果运行该命令的机器上正在运行一个Alluxio worker，那么数据将移动到该worker上，否则，数据会被随机移动到一个worker上。如果该文件已经存在在Alluxio中，该命令不进行任何操作。如果该命令的目标是一个文件夹，那么其子文件和子文件夹会被递归载入。

使用举例：使用`load` 命令能够获取用于数据分析作用的数据。

{% include Command-Line-Interface/load.md %}

## loadMetadata

`loadMetadata`命令查询本地文件系统中匹配给定路径名的所有文件和文件夹，并在Alluxio中创建这些文件的镜像。该命令只创建元数据，例如文件名及文件大小，而不会传输数据。

使用举例：当其他系统将数据输出到底层文件系统中（不经过Alluxio），而在Alluxio上运行的某个应用又需要使用这些输出数据时，就可以使用`loadMetadata`命令。

{% include Command-Line-Interface/loadMetadata.md %}

## location

`location`命令返回包含一个给定文件包含的数据块的所有Alluxio worker的地址。

使用举例：当使用某个计算框架进行作业时，使用`location`命令可以调试数据局部性。

{% include Command-Line-Interface/location.md %}

## ls

`ls`命令列出一个文件夹下的所有子文件和子文件夹及文件大小、上次修改时间以及文件的内存状态。对一个文件使用`ls`命令仅仅会显示该文件的信息。

加上`-R`选项可以递归的列出输入路径下的所有子文件和子文件夹，并列出从输入路径开始的所有子树。

加上`-f`选项可以强制访问本地文件系统, 创建子文件在Alluxio系统中的镜像。该命令只创建元数据, 例如文件名及文件大小，而不会传输数据。如果不加
这个选项, 底层文件只会在第一次访问该文件夹的时候访问一次。

使用举例：使用`ls`命令可以浏览文件系统。

{% include Command-Line-Interface/ls.md %}

## mkdir

`mkdir`命令在Alluxio中创建一个新的文件夹。该命令可以递归创建不存在的父目录。注意在该文件夹中的某个文件被持久化到底层文件系统之前，该文件夹不会在底层文件系统中被创建。对一个无效的或者已存在的路径使用`mkdir`命令会失败。

使用举例：管理员使用`mkdir`命令可以创建一个基本文件夹结构。

{% include Command-Line-Interface/mkdir.md %}

## mount

`mount`命令将一个底层存储中的路径链接到Alluxio路径，并且在Alluxio中该路径下创建的文件和文件夹会在对应的底层文件系统路径进行备份。访问[统一命名空间](Unified-and-Transparent-Namespace.html)获取更多相关信息。

使用举例：使用`mount`命令可以让其他存储系统中的数据在Alluxio中也能获取。

{% include Command-Line-Interface/mount.md %}

## mv

`mv`命令将Alluxio中的文件或文件夹移动到其他路径。目标路径一定不能事先存在或者是一个目录。如果是一个目录，那么该文件或文件夹会成为该目录的子文件或子文件夹。`mv`命令仅仅对元数据进行操作，不会影响该文件的数据块。`mv`命令不能在不同底层存储系统的挂载点之间操作。

使用举例：使用`mv`命令可以将过时数据移动到非工作目录。

{% include Command-Line-Interface/mv.md %}

## persist

`persist`命令将Alluxio中的数据持久化到底层文件系统中。该命令是对数据的操作，因而其执行时间取决于该文件的大小。在持久化结束后，该文件即在底层文件系统中有了备份，因而该文件在Alluxio中的数据块被剔除甚至丢失的情况下，仍能够访问。

使用举例：在从一系列临时文件中过滤出包含有用数据的文件后，便可以使用`persist`命令对其进行持久化。

{% include Command-Line-Interface/persist.md %}

## pin

`pin`命令对Alluxio中的文件或文件夹进行标记。该命令只针对元数据进行操作，不会导致任何数据被加载到Alluxio中。如果一个文件在Alluxio中被标记了，该文件的任何数据块都不会从Alluxio worker中被剔除。如果存在过多的被锁定的文件，Alluxio worker将会剩余少量存储空间，从而导致无法对其他文件进行缓存。

使用举例：如果管理员对作业运行流程十分清楚，那么可以使用`pin`命令手动提高性能。

{% include Command-Line-Interface/pin.md %}

## report

`report`命令向Alluxio master标记一个文件为丢失状态。该命令应当只对使用[Lineage API](Lineage-API.html)创建的文件使用。将一个文件标记为丢失状态将导致master调度重计算作业从而重新生成该文件。

使用举例：使用`report`命令可以强制重新计算生成一个文件。

{% include Command-Line-Interface/report.md %}

## rm

`rm`命令将一个文件从Alluxio以及底层文件系统中删除。该命令返回后该文件便立即不可获取，但实际的数据要过一段时间才被真正删除。

加上`-R`选项可以递归的删除文件夹中所有内容后再删除文件夹自身。

使用举例：使用`rm`命令可以删除掉不再需要的临时文件。

{% include Command-Line-Interface/rm2.md %}

## setTtl

`setTtl`命令设置一个文件的ttl时间，单位为毫秒。当当前时间大于该文件的创建时间与ttl时间之和时，行动参数将指示要执行的操作。`delete`操作（默认）将同时删除Alluxio和底层文件系统中的文件，而`free`操作将仅仅删除Alluxio中的文件。

使用举例：管理员在知道某些文件经过一段时间后便没用时，可以使用带有`delete`操作的`setTtl`命令来清理文件；如果仅仅希望为Alluxio释放更多的空间，可以使用带有`free`操作的`setTtl`命令来清理Alluxio中的文件内容。

{% include Command-Line-Interface/setTtl.md %}

## tail

`tail`命令将一个文件的最后1Kb内容输出到控制台。

使用举例：使用`tail`命令可以确认一个作业的输出是否符合格式或者包含期望的值。

{% include Command-Line-Interface/tail.md %}

## touch

`touch`命令创建一个空文件。由该命令创建的文件不能被覆写，大多数情况是用作标记。

使用举例：使用`touch`命令可以创建一个空文件用于标记一个文件夹的分析任务完成了。

{% include Command-Line-Interface/touch.md %}

## unmount

`unmount`将一个Alluxio路径和一个底层文件系统中的目录的链接断开。该挂载点的所有元数据和文件数据都会被删除，但底层文件系统会将其保留。访问[Unified Namespace](Unified-and-Transparent-Namespace.html)获取更多信息。

使用举例：当不再需要一个底层存储系统中的数据时，使用`unmont`命令可以移除该底层存储系统。

{% include Command-Line-Interface/unmount.md %}

## unpin

`unpin`命令将Alluxio中的文件或文件夹解除标记。该命令仅作用于元数据，不会剔除或者删除任何数据块。一旦文件被解除锁定，Alluxio worker可以剔除该文件的数据块。

使用举例：当管理员知道数据访问模式发生改变时，可以使用`unpin`命令。

{% include Command-Line-Interface/unpin.md %}

## unsetTtl

`unsetTtl`命令删除Alluxio中一个文件的TTL。该命令仅作用于元数据，不会剔除或者删除Alluxio中的数据块。该文件的TTL值可以由`setTtl`命令重新设定。

使用举例：在一些特殊情况下，当一个原本自动管理的文件需要手动管理时，可以使用`unsetTtl`命令。

{% include Command-Line-Interface/unsetTtl.md %}
