#!/usr/bin/env bash

# export AMI_PUBLIC_DNS=`wget -q -O - http://instance-data.ec2.internal/latest/meta-data/public-hostname`

# cd /root/tachyon;

TACH_JAR=/home/haoyuan/Tachyon/tachyon-haoyuan/target/tachyon-0.3.0-SNAPSHOT-jar-with-dependencies.jar

RESULT_FOLDER=/home/haoyuan/Tachyon/tachyon-haoyuan/results

mkdir -p $RESULT_FOLDER

JVM_SIZE=2g

MASTER_IP=localhost:19998

HOSTNAME=`hostname`

# DATA_PATH="hdfs://localhost:54310/performance/$HOSTNAME-"
DATA_PATH="/performance/$HOSTNAME-"

JAVA=/home/haoyuan/tools/jdk1.7.0_25/bin/java

# BLOCKS=4096
BLOCKS=64

for task in {1..1}
do
  sync && echo 3 > /proc/sys/vm/drop_caches
  for i in {0..1}
  do
    j=$(($i*1))
    echo $JAVA -Xmx$JVM_SIZE -Xms$JVM_SIZE -cp $TACH_JAR tachyon.examples.Performance \
      $MASTER_IP $DATA_PATH 262144 $BLOCKS false 1 1 $task $j "&> $RESULT_FOLDER/Task_$task\_$i\_$HOSTNAME.txt" \&
    $JAVA -Xmx$JVM_SIZE -Xms$JVM_SIZE -cp $TACH_JAR tachyon.examples.Performance \
      $MASTER_IP $DATA_PATH 262144 $BLOCKS false 1 1 $task $j &> $RESULT_FOLDER/Task_$task\_$i\_$HOSTNAME.txt &
  done
  # sleep 1
done

# grep "Current System Time" Task_1_* | cut -f1,5,6,38 -d' ' | cut -f2 -d' ' | awk '{s+=$1} END {print s}'

# grep "Exception" *

# for i in {1..6}
# do
#   grep "Current System Time" Task_"$i"_* | cut -f1,5,6,38 -d' ' | cut -f2 -d' ' | awk '{s+=$1} END {print s}'
# done
