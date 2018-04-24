```bash
${ALLUXIO_HOME}/bin/alluxio fs unmount /demo
Unmounted /demo
$ ${ALLUXIO_HOME}/bin/alluxio fs lsr /
... # should not contain /demo
$ ls /tmp/alluxio-demo
hello
```
