cd /spark
if [[ "$SPARK_PROFILE" == "" ]]; then
  ./make-distribution.sh -Dhadoop.version=${HADOOP_VERSION} >/spark/make-distribution.log 2>&1
else
  ./make-distribution.sh -Phadoop-${SPARK_PROFILE} -Dhadoop.version=${HADOOP_VERSION} >/spark/make-distribution.log 2>&1
fi
