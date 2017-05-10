#!/usr/bin/env bash

cd /hibench/conf
cp 99-user_defined_properties.conf.template 99-user_defined_properties.conf
key=("hibench.hadoop.home" "hibench.spark.home" "hibench.hdfs.master" "hibench.spark.master")
value=("/hadoop" "/spark" "tachyon://TachyonMaster:19998" "spark://TachyonMaster:7077")
for ((i=0; i<${#key[@]}; i++)); do
  k=${key[$i]}
  v=${value[$i]}
  sed -i "s|^$k.*|$k $v|g" 99-user_defined_properties.conf
done
