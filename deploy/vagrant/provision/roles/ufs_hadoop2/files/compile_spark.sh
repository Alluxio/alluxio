cd /spark
# for version 2.4.1, major version is 2.4
HADOOP_MAJOR_VERSION=`echo $HADOOP_VERSION | cut -d'.' -f1-2`
if [[ "$SPARK_PROFILE" == "" ]]; then
  ./make-distribution.sh -Dhadoop.version=${HADOOP_VERSION} >/spark/make-distribution.log 2>&1
else
  ./make-distribution.sh -Phadoop-${SPARK_PROFILE} -Dhadoop.version=${HADOOP_VERSION} >/spark/make-distribution.log 2>&1
fi
