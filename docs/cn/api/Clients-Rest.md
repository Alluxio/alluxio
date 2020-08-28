---
layout: global
title: REST API
nickname: REST API
group: Client APIs
priority: 4
---

为了其他语言的可移植性，除使用[原生文件系统Java客户端]({{ '/cn/api/FS-API.html' | relativize_url }})外，也可以通过REST API形式的HTTP代理访问Alluxio文件系统。

[REST API documentation](https://docs.alluxio.io/os/restdoc/{{site.ALLUXIO_MAJOR_VERSION}}/proxy/index.html)作为Alluxio构建的一部分被生成，
可以通过`${ALLUXIO_HOME}/core/server/proxy/target/miredot/index.html`来访问。 REST API和Alluxio Java API之间的主要区别在于如何表示流。Alluxio Java API
可以使用内存中的流，REST API将流的创建和访问分离（有关详细信息请参阅`create`和`open`REST API方法以及`streams`资源端点）。

HTTP代理是一个单机服务器，可以使用`${ALLUXIO_HOME} /bin/alluxio-start.sh proxy`开启服务，使用`${ALLUXIO_HOME} /bin/alluxio-stop.sh proxy`停止服务。默认情况下，REST API可在39999端口访问。

使用HTTP代理有性能问题。特别地，使用代理需要额外的跳跃。为了获得最佳性能，建议在每个计算节点上运行代理服务器和Alluxio工作程序。
