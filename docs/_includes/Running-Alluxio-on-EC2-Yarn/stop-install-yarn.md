```bash
$ cd /alluxio
$ ./bin/alluxio-stop.sh all
$ mvn clean install -Dhadoop.version=2.4.1 -Pyarn -DskipTests -Dfindbugs.skip -Dmaven.javadoc.skip -Dcheckstyle.skip
```
