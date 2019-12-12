---
layout: global
title: Go 客户端
nickname: Go 客户端
group: Client APIs
priority: 8
---

* 内容列表
{:toc}

Alluxio有一个[Go 语言客户端](https://github.com/Alluxio/alluxio-go), 此客户端通过[REST API]({{ '/cn/api/Clients-Rest.html' | relativize_url }})
和Alluxio进行交互。Go 客户端提供一个和[原生文件系统Java客户端]({{ '/cn/api/FS-API.html' | relativize_url }})相似的API。
查看[godoc](http://godoc.org/github.com/Alluxio/alluxio-go)
获取所有可用接口的详细信息，godoc包括如何下载，上传Alluxio中的文件，检查文件是否
存在，列出文件状态等信息。

# Alluxio 代理依赖

Go 语言客户端通过Alluxio代理提供的REST API和Alluxio进行交互。

Alluxio代理是一个独立运行的server，可通过命令`${ALLUXIO_HOME}/bin/alluxio-start.sh proxy`启动，
`${ALLUXIO_HOME}/bin/alluxio-stop.sh proxy`停止服务，默认情况下，REST API使用端口39999。

使用HTTP代理会影响性能，尤其是在使用代理时会增加一个额外的跳计数，所以推荐让代理服务和一个Alluxio worker运行在一个计算节点上。

# 安装Go语言客户端相关库
```console
$ go get -d github.com/Alluxio/alluxio-go
```

# 示例使用程序

如果本地没有Alluxio代理在运行，将下面的"localhost"替换为代理的主机名。

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
