```bash
${ALLUXIO_HOME}/bin/alluxio tfs unmount /demo
> Unmounted /demo
$ ${ALLUXIO_HOME}/bin/alluxio tfs lsr /
... # should not contain /demo
$ ls /tmp/alluxio-demo
> hello
```
