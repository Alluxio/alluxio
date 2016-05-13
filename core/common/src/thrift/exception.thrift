namespace java alluxio.thrift

exception AlluxioTException {
  1: string type // deprecated since 1.1 and will be removed in 2.0
  2: string message
  3: string name
}

exception ThriftIOException {
  1: string message
}

