---
layout: global
title: Go Client
nickname: Go Client
group: Clients
priority: 3
---

Alluxio has a [Go Client](https://github.com/Alluxio/alluxio-go) for interacting with Alluxio through its
[REST API](Clients-Rest.html). The Go client exposes an API similar to the [native Java API](Clients-Java-Native.html).
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

```go
package main

import (
	"fmt"
	"log"

	alluxio "github.com/Alluxio/alluxio-go"
	"github.com/Alluxio/alluxio-go/option"
)

func main() {
	fs := alluxio.NewClient(<proxy-host>, <proxy-port>, <timeout>)
	ok, err := fs.Exists("/test_path", &option.Exists{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(ok)
}
```
