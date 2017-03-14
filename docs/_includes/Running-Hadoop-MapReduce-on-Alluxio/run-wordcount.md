```bash
$ bin/hadoop jar hadoop-examples-1.2.1.jar wordcount -libjars {{site.ALLUXIO_CLIENT_JAR_PATH}} alluxio://localhost:19998/wordcount/input.txt alluxio://localhost:19998/wordcount/output
```
