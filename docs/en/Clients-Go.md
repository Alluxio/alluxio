---
layout: global
title: Go Client
nickname: Go Client
group: Clients
priority: 4
---

* Table of Contents
{:toc}

Alluxio has a [Go Client](https://github.com/Alluxio/alluxio-go) for interacting with Alluxio through its
[REST API](Clients-Rest.html). The Go client exposes an API similar to the [Alluxio Java API](Clients-Alluxio-Java.html).
See the [godoc](http://godoc.org/github.com/Alluxio/alluxio-go) for detailed documentation about all available
methods. The godoc includes examples of how to download, upload, check existence for, and list status for files in
Alluxio.

# Alluxio Proxy dependency

The Go client talks to Alluxio through the REST API provided by the Alluxio proxy.

The proxy is a standalone server that can be started using
`${ALLUXIO_HOME}/bin/alluxio-start.sh proxy` and stopped using `${ALLUXIO_HOME}/bin/alluxio-stop.sh
proxy`. By default, the REST API is available on port 39999.

There are performance implications of using the HTTP proxy. In particular, using the proxy requires
an extra hop. For optimal performance, it is recommended to run the proxy server and an Alluxio
worker on each compute node.

# Install Go Client Library
```bash
$ go get -d github.com/Alluxio/alluxio-go
```

# Example Usage

If there is no Alluxio proxy running locally, replace "localhost" below with a hostname of a proxy.

```go
package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"strings"
	"time"

	alluxio "github.com/alluxio/alluxio-go"
	"github.com/alluxio/alluxio-go/option"
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
