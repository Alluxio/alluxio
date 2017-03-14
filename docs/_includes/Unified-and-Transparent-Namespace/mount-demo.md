```bash
$ ${ALLUXIO_HOME}/bin/alluxio fs mount /demo file:///tmp/alluxio-demo
Mounted file:///tmp/alluxio-demo at /demo
$ ${ALLUXIO_HOME}/bin/alluxio fs lsr /
... # should contain /demo but not /demo/hello
```
