---
layout: global
title: Filesystem API
nickname: Filesystem API
group: APIs
priority: 0
---

Applications primarily interact with Alluxio through its Filesystem API. Java users
can either use the [Alluxio Java Client](#Java-Client), or the 
[Hadoop-Compatible Java Client](#Hadoop-Compatible-Java-Client), which
wraps the Alluxio Java Client to implement the Hadoop API. 

By setting up an Alluxio Proxy, users can also interact with Alluxio through a REST 
API similar to the Filesystem API. The REST API currently has language bindings for 
Go and Python.

A third option is to interact with Alluxio through its S3 API. Users can interact 
using the same S3 clients used for AWS S3 operations. This makes it easy to change
existing S3 workloads to use Alluxio.

* Table of Contents
{:toc}

## Java Client

Alluxio provides access to data through a filesystem interface. Files in Alluxio offer write-once
semantics: they become immutable after they have been written in their entirety and cannot be read
before being completed. Alluxio provides two different Filesystem APIs, the Alluxio API and a Hadoop
compatible API. The Alluxio API provides additional functionality, while the Hadoop compatible API
gives users the flexibility of leveraging Alluxio without having to modify existing code written
using Hadoop's API.

All resources with the Alluxio Java API are specified through a `AlluxioURI` which represents the
path to the resource.

### Getting a Filesystem Client

To obtain an Alluxio filesystem client in Java code, use:

```java
FileSystem fs = FileSystem.Factory.get();
```

### Creating a File

All metadata operations as well as opening a file for reading or creating a file for writing are
executed through the FileSystem object. Since Alluxio files are immutable once written, the
idiomatic way to create files is to use `FileSystem#createFile(AlluxioURI)`, which returns
a stream object that can be used to write the file. For example:

```java
FileSystem fs = FileSystem.Factory.get();
AlluxioURI path = new AlluxioURI("/myFile");
// Create a file and get its output stream
FileOutStream out = fs.createFile(path);
// Write data
out.write(...);
// Close and complete file
out.close();
```

### Specifying Operation Options

For all FileSystem operations, an additional `options` field may be specified, which allows
users to specify non-default settings for the operation. For example:

```java
FileSystem fs = FileSystem.Factory.get();
AlluxioURI path = new AlluxioURI("/myFile");
// Generate options to set a custom blocksize of 128 MB
CreateFileOptions options = CreateFileOptions.defaults().setBlockSize(128 * Constants.MB);
FileOutStream out = fs.createFile(path, options);
```

### IO Options

Alluxio uses two different storage types: Alluxio managed storage and under storage. Alluxio managed
storage is the memory, SSD, and/or HDD allocated to Alluxio workers. Under storage is the storage
resource managed by the underlying storage system, such as S3, Swift or HDFS. Users can specify the
interaction with Alluxio managed storage and under storage through `ReadType` and `WriteType`.
`ReadType` specifies the data read behavior when reading a file. `WriteType` specifies the data
write behavior when writing a new file, ie. whether the data should be written in Alluxio Storage.

Below is a table of the expected behaviors of `ReadType`. Reads will always prefer Alluxio storage
over the under storage.

<table class="table table-striped">
<tr><th>Read Type</th><th>Behavior</th>
</tr>
{% for readtype in site.data.table.ReadType %}
<tr>
  <td>{{readtype.readtype}}</td>
  <td>{{site.data.table.en.ReadType[readtype.readtype]}}</td>
</tr>
{% endfor %}
</table>

Below is a table of the expected behaviors of `WriteType`

<table class="table table-striped">
<tr><th>Write Type</th><th>Behavior</th>
</tr>
{% for writetype in site.data.table.WriteType %}
<tr>
  <td>{{writetype.writetype}}</td>
  <td>{{site.data.table.en.WriteType[writetype.writetype]}}</td>
</tr>
{% endfor %}
</table>

### Location policy

Alluxio provides location policy to choose which workers to store the blocks of a file.

Using Alluxio's Java API, users can set the policy in `CreateFileOptions` for writing files and
`OpenFileOptions` for reading files into Alluxio.

Users can simply override the default policy class in the
[configuration file](Configuration-Settings.html) at property
`alluxio.user.file.write.location.policy.class`. The built-in policies include:

* **LocalFirstPolicy (alluxio.client.file.policy.LocalFirstPolicy)**

    Returns the local host first, and if the local worker doesn't have enough capacity of a block,
    it randomly picks a worker from the active workers list. This is the default policy.

* **MostAvailableFirstPolicy (alluxio.client.file.policy.MostAvailableFirstPolicy)**

    Returns the worker with the most available bytes.

* **RoundRobinPolicy (alluxio.client.file.policy.RoundRobinPolicy)**

    Chooses the worker for the next block in a round-robin manner and skips workers that do not have
    enough capacity.

* **SpecificHostPolicy (alluxio.client.file.policy.SpecificHostPolicy)**

    Returns a worker with the specified host name. This policy cannot be set as default policy.

Alluxio supports custom policies, so you can also develop your own policy appropriate for your
workload by implementing interface `alluxio.client.file.policy.FileWriteLocationPolicy`. Note that a
default policy must have an empty constructor. And to use ASYNC_THROUGH write type, all the blocks
of a file must be written to the same worker.

### Write Tier

Alluxio allows a client to select a tier preference when writing blocks to a local worker. Currently
this policy preference exists only for local workers, not remote workers; remote workers will write
blocks to the highest tier.

By default, data is written to the top tier. Users can modify the default setting through the
`alluxio.user.file.write.tier.default` [configuration](Configuration-Settings.html) property or
override it through an option to the `FileSystem#createFile(AlluxioURI)` API call.

### Accessing an existing file in Alluxio

All operations on existing files or directories require the user to specify the `AlluxioURI`.
With the AlluxioURI, the user may use any of the methods of `FileSystem` to access the resource.

### Reading Data

A `AlluxioURI` can be used to perform Alluxio FileSystem operations, such as modifying the file
metadata, ie. ttl or pin state, or getting an input stream to read the file.

For example, to read a file:

```java
FileSystem fs = FileSystem.Factory.get();
AlluxioURI path = new AlluxioURI("/myFile");
// Open the file for reading
FileInStream in = fs.openFile(path);
// Read data
in.read(...);
// Close file relinquishing the lock
in.close();
```

### Javadoc

For additional API information, please refer to the
[Alluxio javadocs](http://www.alluxio.org/javadoc/{{site.ALLUXIO_MAJOR_VERSION}}/index.html).

## Hadoop-Compatible Java Client

Alluxio provides access to data through a filesystem interface. Files in Alluxio offer write-once
semantics: they become immutable after they have been written in their entirety and cannot be read
before being completed. Alluxio provides two different Filesystem APIs, the Alluxio Filesystem API
and a Hadoop compatible API. The Alluxio API provides additional functionality, while the Hadoop
compatible API gives users the flexibility of leveraging Alluxio without having to modify existing
code written using Hadoop's API.

Alluxio has a wrapper of the [Alluxio client](#Java-Client) which provides the Hadoop
compatible `FileSystem` interface. With this client, Hadoop file operations will be translated to
FileSystem operations. The latest documentation for the `FileSystem` interface may be found
[here](http://hadoop.apache.org/docs/current/api/org/apache/hadoop/fs/FileSystem.html).

The Hadoop compatible interface is provided as a convenience class, allowing users to reuse
previous code written for Hadoop.

## Rest API

For portability with other languages, the [Alluxio API](#Java-Client) is also
accessible via an HTTP proxy in the form of a REST API.

The [REST API documentation](http://www.alluxio.org/restdoc/{{site.ALLUXIO_MAJOR_VERSION}}/proxy/index.html)
is generated as part of Alluxio build and accessible through
`${ALLUXIO_HOME}/core/server/proxy/target/miredot/index.html`. The main difference between
the REST API and the Alluxio Java API is in how streams are represented. While the Alluxio Java API
can use in-memory streams, the REST API decouples the stream creation and access (see the
`create` and `open` REST API methods and the `streams` resource endpoints for details).

The HTTP proxy is a standalone server that can be started using
`${ALLUXIO_HOME}/bin/alluxio-start.sh proxy` and stopped using `${ALLUXIO_HOME}/bin/alluxio-stop.sh
proxy`. By default, the REST API is available on port 39999.

There are performance implications of using the HTTP proxy. In particular, using the proxy requires
an extra hop. For optimal performance, it is recommended to run the proxy server and an Alluxio
worker on each compute node.

## Python

Alluxio has a [Python Client](https://github.com/Alluxio/alluxio-py) for interacting with Alluxio through its
[REST API](Clients-Rest.html). The Python client exposes an API similar to the [Alluxio Java API](Clients-Alluxio-Java.html).
See the [doc](http://alluxio-py.readthedocs.io) for detailed documentation about all available
methods. See the [example](https://github.com/Alluxio/alluxio-py/blob/master/example.py) of how to perform basic filesystem
operations in Alluxio.

### Alluxio Proxy dependency

The Python client interacts with Alluxio through the REST API provided by the Alluxio proxy.

The proxy is a standalone server that can be started using
`${ALLUXIO_HOME}/bin/alluxio-start.sh proxy` and stopped using `${ALLUXIO_HOME}/bin/alluxio-stop.sh
proxy`. By default, the REST API is available on port 39999.

There are performance implications of using the HTTP proxy. In particular, using the proxy requires
an extra hop. For optimal performance, it is recommended to run the proxy server and an Alluxio
worker on each compute node.

### Install Python Client Library
```bash
$ pip install alluxio
```

### Example Usage

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

## Go

Alluxio has a [Go Client](https://github.com/Alluxio/alluxio-go) for interacting with Alluxio through its
[REST API](Clients-Rest.html). The Go client exposes an API similar to the [Alluxio Java API](Clients-Alluxio-Java.html).
See the [godoc](http://godoc.org/github.com/Alluxio/alluxio-go) for detailed documentation about all available
methods. The godoc includes examples of how to download, upload, check existence for, and list status for files in
Alluxio.

### Alluxio Proxy dependency

The Go client talks to Alluxio through the REST API provided by the Alluxio proxy.

The proxy is a standalone server that can be started using
`${ALLUXIO_HOME}/bin/alluxio-start.sh proxy` and stopped using `${ALLUXIO_HOME}/bin/alluxio-stop.sh
proxy`. By default, the REST API is available on port 39999.

There are performance implications of using the HTTP proxy. In particular, using the proxy requires
an extra hop. For optimal performance, it is recommended to run the proxy server and an Alluxio
worker on each compute node.

### Install Go Client Library
```bash
$ go get -d github.com/Alluxio/alluxio-go
```

### Example Usage

If there is no Alluxio proxy running locally, replace "localhost" below with a hostname of a proxy.

```go
package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"strings"
	"time"

	alluxio "github.com/Alluxio/alluxio-go"
	"github.com/Alluxio/alluxio-go/option"
)

func write(fs *alluxio.Client, path, s string) error {
	id, err := fs.CreateFile(path, &option.CreateFile{})
	if err != nil {
		return err
	}
	defer fs.Close(id)
	_, err = fs.Write(id, strings.NewReader(s))
	return err
}

func read(fs *alluxio.Client, path string) (string, error) {
	id, err := fs.OpenFile(path, &option.OpenFile{})
	if err != nil {
		return "", err
	}
	defer fs.Close(id)
	r, err := fs.Read(id)
	if err != nil {
		return "", err
	}
	defer r.Close()
	content, err := ioutil.ReadAll(r)
	if err != nil {
		return "", err
	}
	return string(content), err
}

func main() {
	fs := alluxio.NewClient("localhost", 39999, 10*time.Second)
	path := "/test_path"
	exists, err := fs.Exists(path, &option.Exists{})
	if err != nil {
		log.Fatal(err)
	}
	if exists {
		if err := fs.Delete(path, &option.Delete{}); err != nil {
			log.Fatal(err)
		}
	}
	if err := write(fs, path, "Success"); err != nil {
		log.Fatal(err)
	}
	content, err := read(fs, path)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Result: %v\n", content)
}
```
