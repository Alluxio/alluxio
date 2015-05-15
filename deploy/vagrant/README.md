## What is Vagrant?

Vagrant can create VM images (VirtualBox VMWare Fusion), Docker containers, and AWS and OpenStack
instances.

## Why Use Vagrant?

Installing a Tachyon cluster is a huge undertaking. Building a realistic and correct environment
usually requires multifacted knowledge.

Tachyon uses a variety of under filesystems. Some of these filesystems must be installed and
configured on target systems that are different from development environment. Building identical VMs
across all running environments accelerates development, testing, and adoption process.

## What inside?

This directory contains Vagrant recipe to create VirtualBox images and Amazon EC2 instances and
configurations to initialize Hadoop (both 1.x and 2.x) and GlusterFS.

## Dependencies

1. Vagrant >= 1.6.5
2. Virtualbox if you want a local cluster
3. [Ansible](http://docs.ansible.com/intro_installation.html) for parallel provisioning

## Configuration:

`tachyon/deploy/vagrant/conf/tachyon_version.yml` is the configration file that sets whether you want to use
your local tachyon directory, or clone from a specific commit of a github repo.

`tachyon/deploy/vagrant/conf/spark_version.yml` is the configration file that sets whether you want to set up 
spark, the git repo and version. **Attention**, spark-1.3 should match tachyon-0.5, later spark version matches tachyon versions >= tachyon-0.6.

If you are using spark, better to set memory larger than 2G, otherwise, compiling spark may be blocked.

`tachyon/deploy/vagrant/conf/init.yml` is the configuration file that sets different cluster parameters.
They are explained below.

<table class="table">
<tr>
    <th>Parameter</th><th>Description</th><th>Values</th>
</tr>
<tr>
    <td>Provider</td><td>Vagrant Providers</td><td>vb|aws|openstack|docker</td>
</tr>
<tr>
    <td>Memory</td><td>Memory (in MB) to allocate for Virtualbox image</td><td>at least 1024</td>
</tr>
<tr>
    <td>Total</td><td>Number of images to start</td><td>at least 1</td>
</tr>
<tr>
    <td>Addresses</td><td>Internal IPs given to each VM. The last one is designated as Tachyon master.
For VirtualBox, the addresses can be arbitrary.
For AWS, the addresses should be within the same availability zone.
For OpenStack, since the compute node instances use DHCP, these addresses are not used.
For Docker provider, containers use DHCP, these addresses are not used.
</td><td>IPv4 address string</td>
</tr>
</table>

## Extension

If you can write bash or ansible playbook, you can extend this module by adding
new under layer filesystem or new framework on tachyon. 

You do not need to be an ansible expert, it's enough to know the following conventions:

1. playbook.yml is the entry point of provision by vagrant
2. go through http://docs.ansible.com/playbooks.html to understand
    1. what's playbook
    2. playbook's yaml syntax
    3. `include` expression
    4. variable
    5. `when` expression
    6. module concept, `shell`, `script` module
    7. roles, most importly:
        1. directory structure of roles/xxx, e.x. files, tasks
        2. files under roles/xxx/files/ can be directly referenced from `script` module in tasks
           under roles/xxx/tasks/

Unless the semantic of an ansible module is straightforward and the syntax is much 
simpler than bash, we suggest using bash script with ansible's `shell` module because
it's easier for others to understand since most developers know bash. 


### Add new under layer filesystem

create new directory `roles/ufs_{filesystem_name}` with structure:

	|-- files
	|---- compile_tachyon.sh    # how to compile tachyon against this ufs
	|---- config_tachyon.sh     # there are ufs related configurations in tachyon like TACHYON_UNDERFS_ADDRESS
	|---- ...
	|-- tasks
	|---- download_release.yml  # how to download the binary release of this ufs to a specific directory
	|---- config.yml            # how to configure the ufs
	|---- rsync_dist.yml        # only master will download/compile, how do slaves rsync the binary distribution from master
	|---- start.yml             # how to start the ufs

Then compose the task ymls in playbook.yml

### Add new framework on top of Tachyon

create new file `{framework_name}_version.yml` in the same directory as `spark_version.yml`, 
the file's content should be similar to spark_version.yml, see `Configuration` section for more info.

create new directory `roles/{framework_name}` with structure:

	|-- files
	|---- ...
	|-- tasks
	|---- download_release.yml   # if you want to support deploying releases instead of downloading src code and compile, this file specifies how to download binary release of this framework
	|---- clone_remote_repo.yml  # how to git clone the repo
	|---- compile.yml            # how to compile
	|---- config.yml             # how to configure the framework
	|---- rsync_dist.yml         # only master will download/compile, how do slaves rsync the binary distribution from master
	|---- start.yml              # how to start the framework
	
Then compose the task ymls in playbook.yml

**Interface**

For these two extension tasks, the "Interface" is the combination of:

1. directory structure
2. name and semantic of the yml files 

read existing roles/ufs_xxx and roles/spark, you'll understand the interface. 

**Implementation**

If you need to write bash to implement the interfaces, put them under files/.

If the implementation is related to ufs, put them under ufs_xxx/files, like `ufs_hadoop2/compile_tachyon.sh`, `ufs_hadoop2/compile_spark.sh`, etc. 

Then include the bash scripts with ansible's `script` module in roles/xxx/tasks/*.yml. 

**Conditional Variable**

If you need to use conditional variables when composing tasks in playbook.yml, pass them in `ans.extra_vars` in Vagrantfile.

**Relative Path** 

1. when you want to use files under files directory in tasks directory of the same role, reference the file name directly. 

	e.x. If you want to use `roles/role1/files/shell1.sh` in `roles/role1/tasks/task1.yml`, directly write `shell1.sh` in modules like `script: shell1.sh`, `synchronize: shell1.sh`, `copy: shell1.sh`, etc. Ansible
will take care of relative path referencing for you.

2. other paths in roles/xxx/tasks is relative to current task yml file.
	
	e.x. If `roles/tachyon/tasks/compile.yml` want to include `roles/lib/tasks/maven.yml`, it should write `include: ../../lib/tasks/maven.yml`


## Start a cluster

### VirtualBox Provider

Run command `./run_vb.sh` to start VirtualBox VM. After VM is up, login to
the VM as `root` and password as `vagrant`.

A purple line like `>>> visit 54.200.126.199:19999 for Tachyon Web Console <<<` will be shown to tell you how to access the tachyon web console.

If you choose to set up spark, A purple line like `>>> visit 54.200.126.199:8080 for Spark Web Console <<<` will be shown to tell you how to access the spark web console.

### AWS Provider

Install aws vagrant plugin first. To date, 0.5.0 plugin is tested.

    vagrant plugin install vagrant-aws

Then update configurations in `conf/ec2-config.yml` and shell environment variables `AWS_ACCESS_KEY`
and `AWS_SECRET_KEY`.

Run `./run_aws.sh` to create EC2 VPC instances.

A purple line like `>>> visit 54.200.126.199:19999 for Tachyon Web Console <<<` will be shown to tell you how to access the tachyon web console.

If you choose to set up spark, A purple line like `>>> visit 54.200.126.199:8080 for Spark Web Console <<<` will be shown to tell you how to access the spark web console.

### OpenStack Provider

Install openstack vagrant plugin first. To date, 0.8.0 plugin is tested.

    vagrant plugin install vagrant-openstack-plugin

Then update configurations in `conf/openstack-config.yml` and shell environment variables
`OS_USERNAME` and `OS_PASSWORD`.

Run `run_openstack.sh` to create OpenStack Compute Node instances.

### Docker Provider

Run command `./run_docker.sh` to start containers. After containers are up, login as `root` and
password as `vagrant`.

### Examples of Running VirtualBox Clusters Using Glusterfs as Underfilesystem

Edit `conf/init.yml`. Make sure parameter
`Ufs` is `glusterfs` and `Provider` is `vb`. Change the rest of parameters to what you want if
necessary.

Then start the clusters.

    ./run_vb.sh

### Examples of Running AWS Clusters Using HDFS 2.4 as Underfilesystem

Edit `conf/init.yml`. Make sure parameter `Ufs`
is `hadoop2` and `Provider` is `aws`. Change the rest of parameters, especially network addresses,
to what you want if necessary.

Then start the clusters.

    ./run_aws.sh


### Examples of Running OpenStack Compute Node Clusters Using HDFS 2.4 as Underfilesystem

Edit `conf/init.yml`. Make sure parameter
`Ufs` is `hadoop2` and `Provider` is `openstack`. The `Addresses` are currently not used.

Then start the clusters.

    ./run_openstack.sh


### Examples of Running Docker containers Using HDFS 2.4 as Underfilesystem

Edit `conf/init.yml`. Make sure parameter
`Ufs` is `hadoop2` and `Provider` is `docker`. The `Addresses` are currently not used.

Then start the clusters.

    ./run_docker.sh

## Test AWS deployment performance

`bash util/aws_parallel_perf_test.sh` will deploy a range of size of clusters to AWS, range is specified in the file via `N_INSTANCE_BEG` and `N_INSTANCE_END`, the default values is 2 and 5. This script will use `time` to measure the performance.

## Use Tachyon Cluster

Once clusters are up running, tachyon is installed and configured. The tachyon source directory is
mapped to `/tachyon` directory on each image. Editions are visible on the images.

## Use Spark Cluster

If you specify "Github" as "Type" in spark_version.yml, a spark cluster will be set up, with Tachyon as cache layer and hadoop as underlayer filesystem. 
Spark is installed to `/spark`. You can `vagrant ssh TachyonMaster` to enter the cluster Master Node, then `/spark/bin/spark-shell` to enter spark-shell, 
then you can try it out! 

If you're new to spark, following these links, type in the codes, step by step:
* https://github.com/apache/spark/blob/master/README.md
* http://spark.apache.org/docs/latest/quick-start.html

## Destroy Tachyon Cluster

To stop and destroy the images, run command

    vagrant destroy [-f]
