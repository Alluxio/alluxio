```bash
$ bin/hadoop jar hadoop-examples-1.2.1.jar wordcount -libjars /<PATH_TO_ALLUXIO>/core/client/target/alluxio-core-client-{{site.ALLUXIO_RELEASED_VERSION}}-jar-with-dependencies.jar alluxio://localhost:19998/wordcount/input.txt alluxio://localhost:19998/wordcount/output
```
