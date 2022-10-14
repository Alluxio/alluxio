---
layout: global
title: Rest API
nickname: Rest API
group: Client APIs
priority: 2
---

While users should use [S3 API]({{ '/en/api/S3-API.html' | relativize_url }}) for data I/O operations, admins can 
interact with Alluxio through REST API for actions not supported by S3 API. For example, mount and unmount operations.

* Table of Contents
{:toc}

## Rest API

For portability with other language, the [Alluxio Java API]({{ '/en/api/Java-API.html' | relativize_url }})) is also
accessible via an HTTP proxy in the form of a REST API. Alluxio's Python and Go clients rely on 
this REST API to talk to Alluxio.

The [REST API documentation](https://docs.alluxio.io/os/restdoc/{{site.ALLUXIO_MAJOR_VERSION}}/proxy/index.html)
is generated as part of the Alluxio build and accessible through
`${ALLUXIO_HOME}/core/server/proxy/target/miredot/index.html`. The main difference between
the REST API and the Alluxio Java API is in how streams are represented. While the Alluxio Java API
can use in-memory streams, the REST API decouples the stream creation and access (see the
`create` and `open` REST API methods and the `streams` resource endpoints for details).

The HTTP proxy is a standalone server that can be started using
`${ALLUXIO_HOME}/bin/alluxio-start.sh proxy` and stopped using `${ALLUXIO_HOME}/bin/alluxio-stop.sh
proxy`. By default, the REST API is available on port 39999.

There are performance implications of using the HTTP proxy. In particular, using the proxy requires
an extra network hop to perform filesystem operations. For optimal performance, it is recommended to
run the proxy server and an Alluxio worker on each compute node.

## Python

Alluxio has a [Python Client](https://github.com/Alluxio/alluxio-py) for interacting with Alluxio through its
[REST API](#rest-api). The Python client exposes an API similar to the [Alluxio Java API]({{ '/en/api/Java-API.html' | relativize_url }})).
See the [doc](http://alluxio-py.readthedocs.io) for detailed documentation about all available
methods. See the [example](https://github.com/Alluxio/alluxio-py/blob/master/example.py) on how to perform basic
file system operations in Alluxio.

The Python client requires an Alluxio proxy that exposes the [REST API](#rest-api) to function.

### Install Python Client Library
```console
$ pip install alluxio
```

### Example Usage

The following program includes examples of how to create directory, download, upload, check existence for,
and list status for files in Alluxio.

This example can also be found [here](https://github.com/Alluxio/alluxio-py/blob/master/example.py)
in the Python package's repository.

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
    print(green(s))


def pretty_json(obj):
    return json.dumps(obj, indent=2)


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
print(pretty_json(stat.json()))
info("done")

info("renaming %s to %s" % (py_test, py_test_renamed))
client.rename(py_test, py_test_renamed)
info("done")

info("getting status of %s" % py_test_renamed)
stat = client.get_status(py_test_renamed)
print(pretty_json(stat.json()))
info("done")

info("reading %s" % py_test_renamed)
with client.open(py_test_renamed, 'r') as f:
    print(f.read())
info("done")

info("listing status of paths under /")
root_stats = client.list_status('/')
for stat in root_stats:
    print(pretty_json(stat.json()))
info("done")

info("deleting %s" % py_test_root_dir)
opt = option.Delete(recursive=True)
client.delete(py_test_root_dir, opt)
info("done")

info("asserting that %s is deleted" % py_test_root_dir)
assert not client.exists(py_test_root_dir)
info("done")
```

## Go

Alluxio has a [Go Client](https://github.com/Alluxio/alluxio-go) for interacting with Alluxio through its
[REST API](#rest-api). The Go client exposes an API similar to the [Alluxio Java API](#java-client).
See the [godoc](http://godoc.org/github.com/Alluxio/alluxio-go) for detailed documentation about all available
methods. The godoc includes examples of how to download, upload, check existence for, and list status for files in
Alluxio.

The Go client requires an Alluxio proxy that exposes the [REST API](#rest-api) to function.

### Install Go Client Library
```console
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
