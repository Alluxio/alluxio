```bash
${TACHYON_HOME}/bin/alluxio tfs unmount /demo
> Unmounted /demo
$ ${TACHYON_HOME}/bin/alluxio tfs lsr /
... # should not contain /demo
$ ls /tmp/alluxio-demo
> hello
```
