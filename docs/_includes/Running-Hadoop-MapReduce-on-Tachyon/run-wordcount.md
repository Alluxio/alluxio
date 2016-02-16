```bash
$ bin/hadoop jar hadoop-examples-1.2.1.jar wordcount -libjars /<PATH_TO_TACHYON>/core/client/target/tachyon-core-client-{{site.TACHYON_RELEASED_VERSION}}-jar-with-dependencies.jar -Dtachyon.user.file.understoragetype.default=SYNC_PERSIST tachyon://localhost:19998/wordcount/input.txt tachyon://localhost:19998/wordcount/output
```
