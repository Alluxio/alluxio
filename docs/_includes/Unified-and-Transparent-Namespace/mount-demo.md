```bash
$ ${ALLUXIO_HOME}/bin/alluxio tfs mount /demo /tmp/alluxio-demo
> Mounted /tmp/alluxio-demo at /demo
$ ${ALLUXIO_HOME}/bin/alluxio tfs lsr /
... # should contain /demo but not /demo/hello
```
