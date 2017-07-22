---
layout: global
title: REST API
nickname: REST API
group: Clients
priority: 3
---

For portability with other languages, the Alluxio [native API](Clients-Java-Native.html) is also
accessible via an HTTP proxy in the form of a REST API.

The [REST API documentation](http://www.alluxio.org/restdoc/{{site.ALLUXIO_MAJOR_VERSION}}/proxy/index.html)
is generated as part of Alluxio build and accessible through
`${ALLUXIO_HOME}/core/server/proxy/target/miredot/index.html`. The main difference between
the REST API and the Native API is in how streams are represented. While the native API
can use in-memory streams, the REST API decouples the stream creation and access (see the
`create` and `open` REST API methods and the `streams` resource endpoints for details).

The HTTP proxy is a standalone server that can be started using
`${ALLUXIO_HOME}/bin/alluxio-start.sh proxy` and stopped using `${ALLUXIO_HOME}/bin/alluxio-stop.sh
proxy`. By default, the REST API is available on port 39999.

There are performance implications of using the HTTP proxy. In particular, using the proxy requires
an extra hop. For optimal performance, it is recommended to run the proxy server and an Alluxio
worker on each compute node.
