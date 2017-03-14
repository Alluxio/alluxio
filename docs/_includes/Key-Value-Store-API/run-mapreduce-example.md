```bash
$ export HADOOP_CLASSPATH=${ALLUXIO_INSTALLATION_DIRECTORY}/assembly/target/alluxio-assemblies-${ALLUXIO_VERSION}-jar-with-dependencies.jar

$ ${HADOOP_INSTALLATION_DIRECTORY}/bin/hadoop jar \
  ${ALLUXIO_INSTALLATION_DIRECTORY}/examples/target/alluxio-examples-${ALLUXIO_VERSION}.jar \
  alluxio.examples.keyvalue.hadoop.CloneStoreMapReduce alluxio://${ALLUXIO_MASTER}:${PORT}/${INPUT_KEY_VALUE_STORE_PATH} alluxio://${ALLUXIO_MASTER}:${PORT}/${OUTPUT_KEY_VALUE_STORE_PATH} \
  -libjars=${ALLUXIO_INSTALLATION_DIRECTORY}/assembly/target/alluxio-assemblies-${ALLUXIO_VERSION}-jar-with-dependencies.jar
```
