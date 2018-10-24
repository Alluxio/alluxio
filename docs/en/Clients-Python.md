---
layout: global
title: Python Client
nickname: Python Client
group: Clients
priority: 5
---

* Table of Contents
{:toc}

Alluxio has a [Python Client](https://github.com/Alluxio/alluxio-py) for interacting with Alluxio through its
[REST API](Clients-Rest.html). The Python client exposes an API similar to the [Alluxio Java API](Clients-Alluxio-Java.html).
See the [doc](http://alluxio-py.readthedocs.io) for detailed documentation about all available
methods. See the [example](https://github.com/Alluxio/alluxio-py/blob/master/example.py) of how to perform basic filesystem
operations in Alluxio.

## Alluxio Proxy dependency

The Python client interacts with Alluxio through the REST API provided by the Alluxio proxy.

The proxy is a standalone server that can be started using
`${ALLUXIO_HOME}/bin/alluxio-start.sh proxy` and stopped using `${ALLUXIO_HOME}/bin/alluxio-stop.sh
proxy`. By default, the REST API is available on port 39999.

There are performance implications of using the HTTP proxy. In particular, using the proxy requires
an extra hop. For optimal performance, it is recommended to run the proxy server and an Alluxio
worker on each compute node.

## Install Python Client Library
```bash
$ pip install alluxio
```

## Example Usage

The following program includes examples of how to create directory, download, upload, check existence for,
and list status for files in Alluxio.


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
