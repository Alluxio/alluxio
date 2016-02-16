```bash
${TACHYON_HOME}/bin/tachyon tfs unmount /demo
> Unmounted /demo
$ ${TACHYON_HOME}/bin/tachyon tfs lsr /
... # should not contain /demo
$ ls /tmp/tachyon-demo
> hello
```
