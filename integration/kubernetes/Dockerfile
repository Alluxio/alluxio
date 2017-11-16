FROM csiebler/spark-s3:2.1.0

ARG ALLUXIO_SPARK_JAR=alluxio-1.6.0-spark-client.jar

ADD ${ALLUXIO_SPARK_JAR} /opt/spark/lib/

RUN echo $'\nspark.mesos.executor.docker.volumes /tmp/domain/:/opt/domain/:rw' >> /opt/spark/conf/spark-defaults.conf
