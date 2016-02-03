```bash
$ ${TACHYON_HOME}/bin/tachyon tfs mount /demo /tmp/tachyon-demo
> Mounted /tmp/tachyon-demo at /demo
$ ${TACHYON_HOME}/bin/tachyon tfs lsr /
... # should contain /demo but not /demo/hello
```
