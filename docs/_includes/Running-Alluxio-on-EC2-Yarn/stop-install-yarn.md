```bash
$ cd /alluxio
$ ./bin/alluxio-stop.sh all
$ mvn clean install -Phadoop-2.4 -Dhadoop.version=2.4.1 -Pyarn -Dlicense.skip -DskipTests -Dfindbugs.skip -Dmaven.javadoc.skip -Dcheckstyle.skip
```
