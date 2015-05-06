---
layout: global
title: Deploy Specific Version Of Either Tachyon Or Spark Via Vagrant
---

In deploy/vagrant/tachyon_version.yml:

    # {Github | Local | Release}
    Type: Local

    # github repo and version(can be branch, commit hash)
    Github: 
      Repo: https://github.com/amplab/tachyon
      Version: branch-0.5

    Release:
      Dist: tachyon-0.5.0-bin.tar.gz

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
    <td>Github:Version</td><td>branch or commit hash of the repo</td><td>e.x. branch-0.5 or 88116afd140306ce9755eabe3d6fd305ddaf9267</td>
</tr>
<tr>
    <td>Release:Dist</td><td>the name of tarball you want to deploy</td><td>e.x. tachyon-0.5.0-bin.tar.gz</td>
</tr>
</table>

Similarly, configure deploy/vagrant/spark_version.yml to deploy spark or not:

    # Type can be {None|Github|Release}
    # None means don't set up spark
    # Release means using binary distribution
    Type: None

    Github:
      Repo: https://github.com/apache/spark
      Version: branch-1.3 # repo of this version must contain make-distribution.sh, that is branch-0.8 afterwards
      Version_LessThan_1.0.0: false

    Release: 
      Dist: spark-1.3.1-bin-hadoop1.tgz

The deployed Spark will use Tachyon as Cache layer and Hadoop as underlayer filesystem. 

After configuration, follow [Running-Tachyon-on-VirtualBox](Running-Tachyon-on-VirtualBox.html) or
[Running-Tachyon-on-Container](Running-Tachyon-on-Container.html) or [Running-Tachyon-on-AWS](Running-Tachyon-on-AWS.html)
or [Running-Tachyon-on-OpenStack](Running-Tachyon-on-OpenStack.html)
