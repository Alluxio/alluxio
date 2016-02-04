namespace java alluxio.thrift

exception AlluxioTException {
  1: string type
  2: string message
}

exception ThriftIOException {
  1: string message
}

