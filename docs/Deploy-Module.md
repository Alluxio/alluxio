---
layout: global
title: Index For Deploy Module
---

## Summary
In Tachyon source tree, there is a `deploy` directory which contains `docker` and `vagrant` modules.

## Docker
The `docker` module helps you create a two nodes Tachyon cluster with hadoop as underfs, in the form of docker image. 

Please refer to `deploy/docker/README.md` for more info.

## Vagrant
The `vagrant` module makes deploying a Tachyon cluster with hadoop or glusterfs as underfs, even with Spark run on top of Tachyon, in **just one command** with **simple yaml configuration**. 

No matter you want a local test environment in **virtualbox**, or you want to build a **docker** image, even you need to deploy to **AWS** or **OpenStack**, use the same work flow like `./run_xxx.sh`,
whenever you want to destroy the deployment, just `vagrant destroy`, that's all!

Versions of the full software stack like spark/tachyon/hadoop are configurable through yaml files, for example:

* deploy any branch or commit of Tachyon's official github repo(https://github.com/amplab/tachyon)
* deploy specific release of Tachyon from Tachyon's github release page(https://github.com/amplab/tachyon/releases)
* deploy any version of hadoop release, either it's apache distribution or cloudera distribution
* deploy any branch or commit of Spark's official github repo(https://github.com/apache/spark)
* deploy specific release of Spark from Apache repo(http://archive.apache.org/dist/spark)
* don't deploy Spark

Please refer to `deploy/vagrant/README.md` for more info. Or visit document on this site:

* [Configure Specific Version Of Tachyon Or Spark in Vagrant](Running-Specific-Version-Of-Tachyon-Or-Spark-Via-Vagrant.html)
* [Deploy to Amazon AWS](Running-Tachyon-on-AWS.html)
* [Deploy to OpenStack](Running-Tachyon-on-OpenStack.html)
* [Deploy to Linux Container](Running-Tachyon-on-Container.html)
* [Deploy to VirtualBox](Running-Tachyon-on-VirtualBox.html)

