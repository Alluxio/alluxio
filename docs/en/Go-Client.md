---
layout: global
title: Go Client
nickname: Go Client
group: Clients
priority: 3
---

Alluxio has a [Go Client](https://github.com/Alluxio/alluxio-go) for interacting with Alluxio through it's 
[REST API](Clients-Rest.html). It exposes an API very similar to the native Java API. The Go client requires 
that an Alluxio proxy service is running. See the [godoc](http://godoc.org/github.com/Alluxio/alluxio-go) 
for detailed documentation about all available methods. The godoc includes examples of how to download, 
upload, check existence, and list status for files in Alluxio.

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
