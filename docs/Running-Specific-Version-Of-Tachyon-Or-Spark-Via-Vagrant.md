---
layout: global
title: Deploy Specific Version Of Either Tachyon Or Spark Via Vagrant
---

## deploy/vagrant/conf/tachyon_version.yml:

    # {Github | Local | Release}
    Type: Local

    # github repo and version(can be branch, commit hash)
    Github: 
      Repo: https://github.com/amplab/tachyon
      Version: branch-0.6

    Release:
      Dist: tachyon-0.6.4-bin.tar.gz

Parameters explained: 

<table class="table">
<tr>
    <th>Parameter</th><th>Description</th><th>Values</th>
</tr>
<tr>
    <td>Type</td><td>where to get tachyon</td><td>Local|Github|Release. Local means deploy the tachyon on your host that is deploy/vagrant/../.. relative to where you start vagrant. Github means deploy tachyon from repo and version specified in Github section. Release means deploy binary distribution from https://github.com/amplab/tachyon/releases, Dist specifies which tar you want to download. </td>
</tr>
<tr>
    <td>Github:Repo</td><td>url of github repo for tachyon</td><td>official repo is https://github.com/amplab/tachyon</td>
</tr>
<tr>
    <td>Github:Version</td><td>branch or commit hash of the repo</td><td>e.x. branch-0.6 or 385e66c5dc6ccd1527a66199da0d135f38d3812e</td>
</tr>
<tr>
    <td>Release:Dist</td><td>the name of tarball you want to deploy</td><td>e.x. tachyon-0.6.4-bin.tar.gz</td>
</tr>
</table>

## deploy/vagrant/conf/spark_version.yml

    # Type can be {None|Github|Release}
    # None means don't set up spark
    # Release means using binary distribution
    Type: None

    Github:
      Repo: https://github.com/apache/spark
      Version: branch-1.4 # repo of this version must contain make-distribution.sh, that is branch-0.8 afterwards
      Version_LessThan_1.0.0: false

    Release: 
      Dist: spark-1.4.0-bin-hadoop1.tgz

Parameters' meanings are similar to those of tachyon_version.yml.

The deployed Spark will use Tachyon as Cache layer and Hadoop as underlayer filesystem. 

## deploy/vagrant/conf/ufs_version.yml

    # hadoop1 | hadoop2 | glusterfs
    Type: hadoop2

    Hadoop:
      # apache: http://archive.apache.org/dist/hadoop/common/hadoop-${Version}/hadoop-${Version}.tar.gz
      # cdh: https://repository.cloudera.com/cloudera/cloudera-repos/org/apache/hadoop/hadoop-dist/${Version}/hadoop-dist-${Version}.tar.gz
      # apache | cdh
      Type: cdh
      # e.x. 2.4.1, 2.0.0-cdh4.7.1
      Version: 2.0.0-cdh4.7.1
      # for some version of hadoop, profile is needed when compiling spark
      # https://spark.apache.org/docs/latest/building-spark.html#specifying-the-hadoop-version
      # e.x. 2.4 for hadoop 2.4.1
      # if you don't compile spark, this can be empty
      SparkProfile:

Parameter meaning has been explained in comments of the yml.

After configuration, follow [Running-Tachyon-on-VirtualBox](Running-Tachyon-on-VirtualBox.html) or
[Running-Tachyon-on-Container](Running-Tachyon-on-Container.html) or [Running-Tachyon-on-AWS](Running-Tachyon-on-AWS.html)
or [Running-Tachyon-on-OpenStack](Running-Tachyon-on-OpenStack.html)
