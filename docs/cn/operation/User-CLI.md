---
layout: global
title: 命令行接口
group: Operations
priority: 1
---

* 内容列表
{:toc}

Alluxio命令行接口为用户提供了基本的文件系统操作，可以使用以下命令来得到所有子命令：

```console
$ ./bin/alluxio fs
Usage: alluxio fs [generic options]
       [cat <path>]
       [checkConsistency [-r] <Alluxio path>]
       ...
```

对于用Alluxio URI（如`ls`， `mkdir`）作为参数的`fs`子命令来说，参数应该要么是完整的Alluxio URI `alluxio://<master-hostname>:<master-port>/<path>`，要么是省略了头部信息的`/<path>`，以使用`conf/allluxio-site.properties`中设置的默认的主机名和端口。


>**通配符输入**
>
>大多数需要路径参数的命令可以使用通配符以便简化使用，例如：
>
>```console
>$ ./bin/alluxio fs rm '/data/2014*'
>```
>
>该示例命令会将`data`文件夹下以`2014`为文件名前缀的所有文件删除。
>
>注意有些shell会尝试自动补全输入路径，从而引起奇怪的错误（注意：以下例子中的数字可能不是21，这取决于你的本地文件系统中匹配文件的个数）：
>
>```console
>rm takes 1 arguments,  not 21
>```
>
>作为一种绕开这个问题的方式，你可以禁用自动补全功能（跟具体shell有关，例如`set -f`），或者使用转义通配符，例如：
>
>```console
>$ ./bin/alluxio fs cat /\\*
>```
>
>注意是两个转义符号，这是因为该shell脚本最终会调用一个java程序运行，该java程序将获取到转义输入参数（cat /\\*）。

## 操作列表

<table class="table table-striped">
  <tr><th>操作</th><th>语法</th><th>描述</th></tr>
  {% for item in site.data.table.operation-command %}
    <tr>
      <td>{{ item.operation }}</td>
      <td>{{ item.syntax }}</td>
      <td>{{ site.data.table.cn.operation-command[item.operation] }}</td>
    </tr>
  {% endfor %}
</table>

## 使用示例

### cat

`cat`命令将Alluxio中的一个文件内容全部打印在控制台中，这在用户确认一个文件的内容是否和预想的一致时非常有用。如果你想将文件拷贝到本地文件系统中，使用`copyToLocal`命令。

例如，当测试一个新的计算任务时，`cat`命令可以用来快速确认其输出结果：

```console
$ ./bin/alluxio fs cat /output/part-00000
```

### checkConsistency

`checkConsistency`命令会对比Alluxio和底层存储系统在给定路径下的元数据。如果该路径是一个目录，那么目录下的所有内容都会被对比。该命令会返回所有不一致的文件和目录的列表，系统管理员决定是否对这些不一致数据进行调整。为了避免Alluxio与底层存储系统的元数据不一致，你的系统应该尽量通过Alluxio来修改文件和目录，避免直接访问底层存储系统进行修改。

如果使用了`-r`选项，那么checkConsistency命令会去修复不一致的文件或目录，如果不一致的文件或者文件夹只存在于底层存储系统，那么相应的元数据会被加载到Alluxio中。如果不一致文件的元数据和具体数据已经存在Alluxio中，那么Alluxio会删除具体数据，并且将该文件的元数据重新载入。


注意：该命令需要请求将要被检查的目录子树的读锁，这意味着在该命令完成之前无法对该目录子树的文件或者目录进行写操作或者更新操作。

例如，`checkConsistency`命令可以用来周期性地检查命名空间的完整性：

```console
# List each inconsistent file or directory
$ ./bin/alluxio fs checkConsistency /
#
# Repair the inconsistent files or directories
$ ./bin/alluxio fs checkConsistency -r /
```

### checksum

`checksum`命令输出某个Alluxio文件的md5值。

例如，`checksum`可以用来验证Alluxio中的文件内容与存储在底层文件系统或者本地文件系统中的文件内容是否匹配：

```console
$ ./bin/alluxio fs checksum /LICENSE
md5sum: bf0513403ff54711966f39b058e059a3
md5 LICENSE
MD5 (LICENSE) = bf0513403ff54711966f39b058e059a3
```
### chgrp

`chgrp`命令可以改变Alluxio中的文件或文件夹的所属组，Alluxio支持POSIX标准的文件权限，组在POSIX文件权限模型中是一个授权实体，文件所有者或者超级用户可以执行这条命令从而改变一个文件或文件夹的所属组。

加上`-R`选项可以递归的改变文件夹中子文件和子文件夹的所属组。

使用举例：使用`chgrp`命令能够快速修改一个文件的所属组：

```console
$ ./bin/alluxio fs chgrp alluxio-group-new /input/file1
```
### chmod

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

```console
$ ./bin/alluxio fs chmod 755 /input/file1
```
### chown

`chown`命令用于修改Alluxio中文件或文件夹的所有者，出于安全方面的考虑，只有超级用户能够更改一个文件的所有者。

加上`-R`选项可以递归的改变文件夹中子文件和子文件夹的所有者。

使用举例：使用`chown`命令可以快速修改一个文件的所有者。

```console
$ ./bin/alluxio fs chown alluxio-user /input/file1
$ ./bin/alluxio fs chown alluxio-user:alluxio-group /input/file2
```
### copyFromLocal

`copyFromLocal`命令将本地文件系统中的文件拷贝到Alluxio中，如果你运行该命令的机器上有Alluxio worker，那么数据便会存放在这个worker上，否则，数据将会随机地复制到一个运行Alluxio worker的远程节点上。如果该命令指定的目标是一个文件夹，那么这个文件夹及其所有内容都会被递归复制到Alluxio中。

使用举例：使用`copyFromLocal`命令可以快速将数据复制到alluxio系统中以便后续处理：

```console
$ ./bin/alluxio fs copyFromLocal /local/data /input
```
### copyToLocal

`copyToLocal`命令将Alluxio中的文件复制到本地文件系统中，如果该命令指定的目标是一个文件夹，那么该文件夹及其所有内容都会被递归地复制。

使用举例：使用`copyToLocal`命令可以快速将输出数据下载下来从而进行后续研究或调试：

```console
$ ./bin/alluxio fs copyToLocal /output/part-00000 part-00000
$ wc -l part-00000
```

### count

`count`命令输出Alluxio中所有名称匹配一个给定前缀的文件及文件夹的总数，以及它们总的大小，该命令对文件夹中的内容递归处理。当用户对文件有预定义命名习惯时，`count`命令很有用。

使用举例：若文件是以它们的创建日期命名，使用`count`命令可以获取任何日期、月份以及年份的所有文件的数目以及它们的总大小：

```console
$ ./bin/alluxio fs count /data/2014
```

### cp

`cp`命令拷贝Alluxio文件系统中的一个文件或者目录,也可以在本地文件系统和Alluxio文件系统之间相互拷贝。

`file`scheme表示本地文件系统，`alluxio`scheme或不写scheme表示Alluxio文件系统。

如果使用了`-R`选项，并且源路径是一个目录，`cp`将源路径下的整个子树拷贝到目标路径。

例如，`cp`可以在底层文件系统之间拷贝文件。

```console
$ ./bin/alluxio fs cp /hdfs/file1 /s3/
```

### du

`du`命令输出一个文件的大小，如果指定的目标为文件夹，该命令输出该文件夹下所有子文件及子文件夹中内容的大小总和。

使用举例：如果Alluxio空间被过分使用，使用`du`命令可以检测到哪些文件夹占用了大部分空间：

```console
# Shows the size information of all the files in root directory
$ ./bin/alluxio fs du /
File Size     In Alluxio       Path
1337          0 (0%)           /alluxio-site.properties
4352          4352 (100%)      /testFolder/NOTICE
26847         0 (0%)           /testDir/LICENSE
2970          2970 (100%)      /testDir/README.md

# Shows the in memory size information
$ ./bin/alluxio fs du --memory /
File Size     In Alluxio       In Memory        Path
1337          0 (0%)           0 (0%)           /alluxio-site.properties
4352          4352 (100%)      4352 (100%)      /testFolder/NOTICE
26847         0 (0%)           0 (0%)           /testDir/LICENSE
2970          2970 (100%)      2970 (100%)      /testDir/README.md

# Shows the aggregate size information in human-readable format
$ ./bin/alluxio fs du -h -s /
File Size     In Alluxio       In Memory        Path
34.67KB       7.15KB (20%)     7.15KB (20%)     /

# Can be used to detect which folders are taking up the most space
$ ./bin/alluxio fs du -h -s /\\*
File Size     In Alluxio       Path
1337B         0B (0%)          /alluxio-site.properties
29.12KB       2970B (9%)       /testDir
4352B         4352B (100%)     /testFolder
```

### fileInfo
`fileInfo`命令从1.5开始不再支持，请使用stat命令。

`fileInfo`命令将一个文件的主要信息输出到控制台，这主要是为了让用户调试他们的系统。一般来说，在Web UI上查看文件信息要容易理解得多。

使用举例：使用`fileInfo`命令能够获取到一个文件的数据块的位置，这在获取计算任务中的数据局部性时非常有用。

```console
$ ./bin/alluxio fs fileInfo /data/2015/logs-1.txt
```

### free

`free`命令请求Alluxio master将一个文件的所有数据块从Alluxio worker中剔除，如果命令参数为一个文件夹，那么会递归作用于其子文件和子文件夹。该请求不保证会立即产生效果，因为该文件的数据块可能正在被读取。`free`命令在被master接收后会立即返回。注意该命令不会删除底层文件系统中的任何数据，而只会影响存储在Alluxio中的数据。另外，该操作也不会影响元数据，这意味着如果运行`ls`命令，该文件仍然会被显示。

使用举例：使用`free`命令可以手动管理Alluxio的数据缓存。

```console
$ ./bin/alluxio fs free /unused/data
```

### getCapacityBytes

`getCapacityBytes`命令返回Alluxio被配置的最大字节数容量。

使用举例：使用`getCapacityBytes`命令能够确认你的系统是否正确启动。

```console
$ ./bin/alluxio fs getCapacityBytes
```

### getUsedBytes

`getUsedBytes`命令返回Alluxio中以及使用的空间字节数。

使用举例：使用`getUsedBytes`命令能够监控集群健康状态。

```console
$ ./bin/alluxio fs getUsedBytes
```

### help

`help`命令对一个给定的`fs`子命令打印帮助信息。如果没有给定，则打印所有支持的子命令的帮助信息。

使用举例：

```console
# 打印所有子命令
$ ./bin/alluxio fs help
#
# 对 ls 命令打印帮助信息
$ ./bin/alluxio fs help ls
```

### leader

`leader`命令打印当前Alluxio的leader master节点主机名。

```console
$ ./bin/alluxio fs leader
```

### load

`load` 命令将底层文件系统中的数据载入到Alluxio中。如果运行该命令的机器上正在运行一个Alluxio worker，那么数据将移动到该worker上，否则，数据会被随机移动到一个worker上。
如果该文件已经存在在Alluxio中，设置了`--local`选项，并且有本地worker，则数据将移动到该worker上。
否则该命令不进行任何操作。如果该命令的目标是一个文件夹，那么其子文件和子文件夹会被递归载入。

使用举例：使用`load` 命令能够获取用于数据分析作用的数据。

```console
$ ./bin/alluxio fs load /data/today
```

### loadMetadata

`loadMetadata`命令查询本地文件系统中匹配给定路径名的所有文件和文件夹，并在Alluxio中创建这些文件的镜像。该命令只创建元数据，例如文件名及文件大小，而不会传输数据。

使用举例：当其他系统将数据输出到底层文件系统中（不经过Alluxio），而在Alluxio上运行的某个应用又需要使用这些输出数据时，就可以使用`loadMetadata`命令。

```console
$ ./bin/alluxio fs loadMetadata /hdfs/data/2015/logs-1.txt
```

### location

`location`命令返回包含一个给定文件包含的数据块的所有Alluxio worker的地址。

使用举例：当使用某个计算框架进行作业时，使用`location`命令可以调试数据局部性。

```console
$ ./bin/alluxio fs location /data/2015/logs-1.txt
```

### ls

`ls`命令列出一个文件夹下的所有子文件和子文件夹及文件大小、上次修改时间以及文件的内存状态。对一个文件使用`ls`命令仅仅会显示该文件的信息。
`ls`命令也将任意文件或者目录下的子目录的元数据从底层存储系统加载到Alluxio命名空间，如果Alluxio还没有这部分元数据的话。
`ls`命令查询底层文件系统中匹配给定路径的文件或者目录，然后会在Alluxio中创建一个该文件的镜像文件。只有元数据，比如文件名和大小，会以这种方式加载而不发生数据传输。

选项：

* `-d` 选项将目录作为普通文件列出。例如，`ls -d /`显示根目录的属性。
* `-f` 选项强制加载目录中的子目录的元数据。默认方式下，只有当目录首次被列出时，才会加载元数据。
* `-h` 选项以可读方式显示文件大小。
* `-p` 选项列出所有固定的文件。
* `-R` 选项可以递归的列出输入路径下的所有子文件和子文件夹，并列出从输入路径开始的所有子树。
* `--sort` 按给定的选项对结果进行排序。可能的值：`size|creationTime|inMemoryPercentage|lastModificationTime|path`
* `-r` 反转排序的顺序。


使用举例：使用`ls`命令可以浏览文件系统。

```console
$ ./bin/alluxio fs mount /s3/data s3://data-bucket/
# Loads metadata for all immediate children of /s3/data and lists them.
$ ./bin/alluxio fs ls /s3/data/
#
# Forces loading metadata.
$ aws s3 cp /tmp/somedata s3://data-bucket/somedata
$ ./bin/alluxio fs ls -f /s3/data
#
# Files are not removed from Alluxio if they are removed from the UFS (s3 here) only.
$ aws s3 rm s3://data-bucket/somedata
$ ./bin/alluxio fs ls -f /s3/data
```

### masterInfo

`masterInfo`命令打印与Alluxio master容错相关的信息，例如leader的地址、所有master的地址列表以及配置的Zookeeper地址。如果Alluxio运行在单master模式下，`masterInfo`命令会打印出该master的地址；如果Alluxio运行在多master容错模式下，`masterInfo`命令会打印出当前的leader地址、所有master的地址列表以及Zookeeper的地址。

使用举例：使用`masterInfo`命令可以打印与Alluxio master容错相关的信息。

```console
$ ./bin/alluxio fs masterInfo
```

### mkdir

`mkdir`命令在Alluxio中创建一个新的文件夹。该命令可以递归创建不存在的父目录。注意在该文件夹中的某个文件被持久化到底层文件系统之前，该文件夹不会在底层文件系统中被创建。对一个无效的或者已存在的路径使用`mkdir`命令会失败。

使用举例：管理员使用`mkdir`命令可以创建一个基本文件夹结构。

```console
$ ./bin/alluxio fs mkdir /users
$ ./bin/alluxio fs mkdir /users/Alice
$ ./bin/alluxio fs mkdir /users/Bob
```

### mount

`mount`命令将一个底层存储中的路径链接到Alluxio路径，并且在Alluxio中该路径下创建的文件和文件夹会在对应的底层文件系统路径进行备份。访问[统一命名空间](Unified-and-Transparent-Namespace.html)获取更多相关信息。

选项：

* `--readonly` 选项在Alluxio中设置挂载点为只读
* `--option <key>=<val>` 选项传递一个属性到这个挂载点(如 S3 credential)

使用举例：使用`mount`命令可以让其他存储系统中的数据在Alluxio中也能获取。

```console
$ ./bin/alluxio fs mount /mnt/hdfs hdfs://host1:9000/data/
$ ./bin/alluxio fs mount --shared --readonly /mnt/hdfs2 hdfs://host2:9000/data/
$ ./bin/alluxio fs mount \
--option aws.accessKeyId=<accessKeyId> \
--option aws.secretKey=<secretKey> \
/mnt/s3 s3://data-bucket/
```

### mv

`mv`命令将Alluxio中的文件或文件夹移动到其他路径。目标路径一定不能事先存在或者是一个目录。如果是一个目录，那么该文件或文件夹会成为该目录的子文件或子文件夹。`mv`命令仅仅对元数据进行操作，不会影响该文件的数据块。`mv`命令不能在不同底层存储系统的挂载点之间操作。

使用举例：使用`mv`命令可以将过时数据移动到非工作目录。

```console
$ ./bin/alluxio fs mv /data/2014 /data/archives/2014
```

### persist

`persist`命令将Alluxio中的数据持久化到底层文件系统中。该命令是对数据的操作，因而其执行时间取决于该文件的大小。在持久化结束后，该文件即在底层文件系统中有了备份，因而该文件在Alluxio中的数据块被剔除甚至丢失的情况下，仍能够访问。

使用举例：在从一系列临时文件中过滤出包含有用数据的文件后，便可以使用`persist`命令对其进行持久化。

```console
$ ./bin/alluxio fs persist /tmp/experimental-logs-2.txt
```

### pin

`pin`命令对Alluxio中的文件或文件夹进行标记。该命令只针对元数据进行操作，不会导致任何数据被加载到Alluxio中。如果一个文件在Alluxio中被标记了，该文件的任何数据块都不会从Alluxio worker中被剔除。如果存在过多的被锁定的文件，Alluxio worker将会剩余少量存储空间，从而导致无法对其他文件进行缓存。

使用举例：如果管理员对作业运行流程十分清楚，那么可以使用`pin`命令手动提高性能。

```console
$ ./bin/alluxio fs pin /data/today
```

### rm

`rm`命令将一个文件从Alluxio以及底层文件系统中删除。该命令返回后该文件便立即不可获取，但实际的数据要过一段时间才被真正删除。

加上`-R`选项可以递归的删除文件夹中所有内容后再删除文件夹自身。加上`-U`选项将会在尝试删除持久化目录之前不会检查将要删除的UFS内容是否与Alluxio一致。

使用举例：使用`rm`命令可以删除掉不再需要的临时文件。

```console
$ ./bin/alluxio fs rm '/data/2014*'
```

### setTtl

`setTtl`命令设置一个文件或者文件夹的ttl时间，单位为毫秒。如果当前时间大于该文件的创建时间与ttl时间之和时，行动参数将指示要执行的操作。**delete操作（默认）将同时删除Alluxio和底层文件系统中的文件**，而`free`操作将仅仅删除Alluxio中的文件。

使用举例：管理员在知道某些文件经过一段时间后便没用时，可以使用带有`delete`操作的`setTtl`命令来清理文件；如果仅仅希望为Alluxio释放更多的空间，可以使用带有`free`操作的`setTtl`命令来清理Alluxio中的文件内容。

```console
# After 1 day, delete the file in Alluxio and UFS
$ ./bin/alluxio fs setTtl /data/good-for-one-day 86400000
# After 1 day, free the file from Alluxio
$ ./bin/alluxio fs setTtl --action free /data/good-for-one-day 86400000
```

### stat

`stat`命令将一个文件或者文件夹的主要信息输出到控制台，这主要是为了让用户调试他们的系统。一般来说，在Web UI上查看文件信息要容易理解得多。

可以指定 `-f <arg>` 来按指定格式显示信息：
* "%N": 文件名;
* "%z": 文件大小(bytes);
* "%u": 文件拥有者;
* "%g": 拥有者所在组名;
* "%y" or "%Y": 编辑时间, %y shows 'yyyy-MM-dd HH:mm:ss' (the UTC
date), %Y 为自从 January 1, 1970 UTC 以来的毫秒数;
* "%b": 为文件分配的数据块数

例如，使用`stat`命令能够获取到一个文件的数据块的位置，这在获取计算任务中的数据局部性时非常有用。

```console
# Displays file's stat
$ ./bin/alluxio fs stat /data/2015/logs-1.txt
#
# Displays directory's stat
$ ./bin/alluxio fs stat /data/2015
#
# Displays the size of file
$ ./bin/alluxio fs stat -f %z /data/2015/logs-1.txt
```

### tail

`tail`命令将一个文件的最后1Kb内容输出到控制台。

使用举例：使用`tail`命令可以确认一个作业的输出是否符合格式或者包含期望的值。

```console
$ ./bin/alluxio fs tail /output/part-00000
```

### test

`test`命令测试路径的属性，如果属性为真，返回0，否则返回1。
可以使用`-d`选项测试路径是否是目录，使用`-f`选项测试路径是否是文件，使用`-e`选项测试路径是否存在，使用`-z`选项测试文件长度是否为0，使用`-s`选项测试路径是否为空，

选项：

 * `-d` 选项测试路径是否是目录。
 * `-e` 选项测试路径是否存在。
 * `-f` 选项测试路径是否是文件。
 * `-s` 选项测试路径是否为空。
 * `-z` 选项测试文件长度是否为0。

使用举例:

```console
$ ./bin/alluxio fs test -d /someDir
$ echo $?
```

### touch

`touch`命令创建一个空文件。由该命令创建的文件不能被覆写，大多数情况是用作标记。

使用举例：使用`touch`命令可以创建一个空文件用于标记一个文件夹的分析任务完成了。

```console
$ ./bin/alluxio fs touch /data/yesterday/_DONE_
```
### unmount

`unmount`将一个Alluxio路径和一个底层文件系统中的目录的链接断开。该挂载点的所有元数据和文件数据都会被删除，但底层文件系统会将其保留。访问[Unified Namespace](Unified-and-Transparent-Namespace.html)获取更多信息。

使用举例：当不再需要一个底层存储系统中的数据时，使用`unmont`命令可以移除该底层存储系统。

```console
$ ./bin/alluxio fs unmount /s3/data
```

### unpin

`unpin`命令将Alluxio中的文件或文件夹解除标记。该命令仅作用于元数据，不会剔除或者删除任何数据块。一旦文件被解除锁定，Alluxio worker可以剔除该文件的数据块。

使用举例：当管理员知道数据访问模式发生改变时，可以使用`unpin`命令。

```console
$ ./bin/alluxio fs unpin /data/yesterday/join-table
```

### unsetTtl

`unsetTtl`命令删除Alluxio中一个文件的TTL。该命令仅作用于元数据，不会剔除或者删除Alluxio中的数据块。该文件的TTL值可以由`setTtl`命令重新设定。

使用举例：在一些特殊情况下，当一个原本自动管理的文件需要手动管理时，可以使用`unsetTtl`命令。

```console
$ ./bin/alluxio fs unsetTtl /data/yesterday/data-not-yet-analyzed
```
