---
layout: global
title: Deploy Specific Version Of Either Tachyon Or Spark Via Vagrant
---

In deploy/vagrant/tachyon_version.yml:

    # {Github | Local}
    Type: Github

    # github repo and commit hash 
    Github: 
      Repo: https://github.com/amplab/tachyon
      Version: eaeaccd19832494c5f0b2a792888a5eaacd7bcf8

You can set `Type` to `Local` to deploy the Tachyon built locally in your Tachyon project's root directory

Set `Type` to `Github`, `Repo` to the git repo url, `Version` to commit hash or remote branch name, then deploy this specific version of Tachyon!

Similarly, configure deploy/vagrant/spark_version.yml to deploy spark or not:

    # Type can be {None|Github}, None means don't set up spark
    Type: None
    Github:
      Repo: https://github.com/apache/spark
      Version: branch-1.3 # repo of this version must contain make-distribution.sh, that is branch-0.8 afterwards
      Version_LessThan_1.0.0: false

The deployed Spark will use Tachyon as Cache layer and Hadoop as underlayer filesystem. 

After configuration, follow [Running-Tachyon-on-VirtualBox](Running-Tachyon-on-VirtualBox.html) or
[Running-Tachyon-on-Container](Running-Tachyon-on-Container.html) or [Running-Tachyon-on-AWS](Running-Tachyon-on-AWS.html)
or [Running-Tachyon-on-OpenStack](Running-Tachyon-on-OpenStack.html)
