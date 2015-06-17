This doc is for contributors on deploy module, for users, please go to the [online doc](http://tachyon-project.org/master/Deploy-Module.html).

## Tools

### Vagrant

[Vagrant](https://www.vagrantup.com), here is the [download link](https://www.vagrantup.com/downloads.html).

#### What is Vagrant?

Vagrant can create VM images (VirtualBox, VMWare Fusion), Docker containers, and AWS and OpenStack
instances.

#### Why Use Vagrant?

Setting up a Tachyon cluster correctly with under filesystem and computation frameworks is a tedious huge undertaking. It requires not only expertise on both Tachyon and the related systems, but also expertise on the target deployment platform. 

Vagrant makes it possible to predefine how to install and configure software on a "machine" which can be an aws instance, a virtualbox vm, a docker container or an openstack instance. Then with the same workflow, you can create the same environment on all these platforms.

### Ansible

[Ansible](http://docs.ansible.com), here is the [download link](http://docs.ansible.com/intro_installation.html)

Ansible is a pure python package, so you need to have python installed, follow the docs in the link above. 

#### What is Ansible?

Ansible is a language and toolset to define how to provision(install software, configure the system). It allows you to manipulate remote servers on your laptop, any number of nodes in parallel!

#### Why Use Ansible?

When set up a Tachyon cluster, we need the ability to manipulate target machines in parallel, say, install java on all nodes. Ansible satisfies this requirement. It's supported by Vagrant and has simple syntax, so we adopt it as the provisioning tool.

## Extension

You can extend deploy module by adding
new under layer filesystems or new computation frameworks on top of tachyon. 

It's enough for you to be able to write bash and know [what is ansible playbook](http://docs.ansible.com/playbooks.html). Then read `deploy/vagrant/provision` directory to make sure you understand existing code base. 

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

Then compose the task ymls in playbook.yml, configurations should be in `conf/ufs.yml`.

### Add new framework on top of Tachyon

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
	
Then compose the task ymls in playbook.yml, configurations should be in `conf/{framework_name}.yml`.

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


