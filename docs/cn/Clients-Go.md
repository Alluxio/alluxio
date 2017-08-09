---
layout: global
title: Go 客户端
nickname: Go 客户端
group: Clients
priority: 3
---

Alluxio有一个[Go Client](https://github.com/Alluxio/alluxio-go), 此客户端通过[REST API](Clients-Rest.html)
和Alluxio进行交互。Go 客户端提供一个和[native Java API](Clients-Java-Native.html)相似的API。
查看[godoc](http://godoc.org/github.com/Alluxio/alluxio-go)
获取所有可用接口的详细信息，godoc包括如何下载，上传Alluxio中的文件，检查文件是否
存在，列出文件状态等信息。

# Alluxio 代理依赖

Go 客户端通过Alluxio代理提供的REST API和Alluxio进行交互。

Alluxio代理是一个独立运行的server，可用通过`${ALLUXIO_HOME}/bin/alluxio-start.sh proxy`进行启动，
`${ALLUXIO_HOME}/bin/alluxio-stop.sh proxy`停止服务，默认情况下，REST API使用端口39999.

使用HTTP代理会影响性能，尤其是，使用代理会增加一个额外的跳计数，所以推荐代理服务和一个Alluxio worker运行在一个计算节点上。

# 安装Go客户端相关库
```bash
$ go get -d github.com/Alluxio/alluxio-go
```

# 示例使用程序

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
