#!/usr/bin/env bash

# export AMI_PUBLIC_DNS=`wget -q -O - http://instance-data.ec2.internal/latest/meta-data/public-hostname`

# cd /root/tachyon;

mkdir -p results;

mem_size=2g

MASTER_IP=localhost:19998

HOSTNAME=`hostname`

PATH=hdfs://localhost:54310/performance/$HOSTNAME_

JAVA=/home/haoyuan/tools/jdk1.7.0_25/bin/java
# BLOCKS=4096
BLOCKS=64

for task in {8..8}
do
  sync && echo 3 > /proc/sys/vm/drop_caches
  for i in {0..1}
  do
    j=$(($i*1))
    echo $JAVA -Xmx$mem_size -Xms$mem_size -cp target/tachyon-0.3.0-SNAPSHOT-jar-with-dependencies.jar \
      tachyon.examples.Performance $MASTER_IP $PATH 262144 $BLOCKS false 1 1 $task $j "&> results/Task_$task\_$i\_$HOSTNAME.txt" \&
    $JAVA -Xmx$mem_size -Xms$mem_size -cp target/tachyon-0.3.0-SNAPSHOT-jar-with-dependencies.jar \
      tachyon.examples.Performance $MASTER_IP $PATH 262144 $BLOCKS false 1 1 $task $j &> results/Task_$task\_$i\_$HOSTNAME.txt &
  done
  # sleep 1
done

# grep "Current System Time" Task_1_* | cut -f1,5,6,38 -d' ' | cut -f2 -d' ' | awk '{s+=$1} END {print s}'

# grep "Exception" *

# for i in {1..6}
# do
#   grep "Current System Time" Task_"$i"_* | cut -f1,5,6,38 -d' ' | cut -f2 -d' ' | awk '{s+=$1} END {print s}'
# done
