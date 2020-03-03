---
layout: global
title: Python Client
nickname: Python Client
group: Client APIs
priority: 6
---

* 内容列表
{:toc}

Alluxio 有一个 [Python 客户端](https://github.com/Alluxio/alluxio-py)，这个客户端提供了 [REST API]({{ '/cn/api/Clients-Rest.html' | relativize_url }}) 来和 Alluxio 交互。它提供了一个和 [Alluxio Java API](Clients-Alluxio-Java.html) 类似的 API。查看这篇[文档](http://alluxio-py.readthedocs.io)来了解有关所有可用方法的详细文档。通过[示例](https://github.com/Alluxio/alluxio-py/blob/master/example.py)来了解如何在 Alluxio 上执行基本的文件系统操作。

# Alluxio代理依赖
这个Python客户端通过由Alluxio代理提供的REST API来和Alluxio相互交流。
这个代理服务器是一个独立的服务器，可以通过`${ALLUXIO_HOME}/bin/alluxio-start.sh proxy`来开启它和通过命令`${ALLUXIO_HOME}/bin/alluxio-stop.sh proxy`来关闭它。默认情况下，可以通过端口39999来使用REST API。
使用HTTP代理服务器有性能上的影响。特别的是，使用这个代理服务器需要一个额外的跳。为了达到最佳性能，运行这个代理服务器的时候推荐在每个计算节点上分配一个Alluxio worker。

# 安装python客户端库
```console
$ pip install alluxio
```

# 使用示例
下面的程序示例包括了如何在Alluxio创建目录、下载、上传、检查文件是否存在以及文件列表状态。

```python
#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import sys

import alluxio
from alluxio import option


def colorize(code):
    def _(text, bold=False):
        c = code
        if bold:
            c = '1;%s' % c
        return '\033[%sm%s\033[0m' % (c, text)
    return _

green = colorize('32')


def info(s):
    print green(s)


def pretty_json(obj):
    return json.dumps(obj, indent=2)


def main():
    py_test_root_dir = '/py-test-dir'
    py_test_nested_dir = '/py-test-dir/nested'
    py_test = py_test_nested_dir + '/py-test'
    py_test_renamed = py_test_root_dir + '/py-test-renamed'

    client = alluxio.Client('localhost', 39999)

    info("creating directory %s" % py_test_nested_dir)
    opt = option.CreateDirectory(recursive=True)
    client.create_directory(py_test_nested_dir, opt)
    info("done")

    info("writing to %s" % py_test)
    with client.open(py_test, 'w') as f:
        f.write('Alluxio works with Python!\n')
        with open(sys.argv[0]) as this_file:
            f.write(this_file)
    info("done")

    info("getting status of %s" % py_test)
    stat = client.get_status(py_test)
    print pretty_json(stat.json())
    info("done")

    info("renaming %s to %s" % (py_test, py_test_renamed))
    client.rename(py_test, py_test_renamed)
    info("done")

    info("getting status of %s" % py_test_renamed)
    stat = client.get_status(py_test_renamed)
    print pretty_json(stat.json())
    info("done")

    info("reading %s" % py_test_renamed)
    with client.open(py_test_renamed, 'r') as f:
        print f.read()
    info("done")

    info("listing status of paths under /")
    root_stats = client.list_status('/')
    for stat in root_stats:
        print pretty_json(stat.json())
    info("done")

    info("deleting %s" % py_test_root_dir)
    opt = option.Delete(recursive=True)
    client.delete(py_test_root_dir, opt)
    info("done")

    info("asserting that %s is deleted" % py_test_root_dir)
    assert not client.exists(py_test_root_dir)
    info("done")


if __name__ == '__main__':
    main()

```
