```bash
$ ${ALLUXIO_HOME}/bin/alluxio fs mount /demo /tmp/alluxio-demo
> Mounted /tmp/alluxio-demo at /demo
$ ${ALLUXIO_HOME}/bin/alluxio fs lsr /
... # should contain /demo but not /demo/hello
```
