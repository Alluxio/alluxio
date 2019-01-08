[![logo](docs/resources/alluxio_logo.png "Alluxio")](https://www.alluxio.org)

[![Slack](https://img.shields.io/badge/slack-alluxio-blue.svg?logo=slack)](https://www.alluxio.org/slack)
[![Release](https://img.shields.io/github/release/alluxio/alluxio/all.svg)](https://www.alluxio.org/download)
[![Docker Pulls](https://img.shields.io/docker/pulls/alluxio/alluxio.svg)](https://hub.docker.com/r/alluxio/alluxio)
[![Documentation](https://img.shields.io/badge/docs-reference-blue.svg)](https://www.alluxio.org/docs)
[![Twitter Follow](https://img.shields.io/twitter/follow/alluxio.svg?label=Follow&style=social)](https://twitter.com/intent/follow?screen_name=alluxio)
[![License](https://img.shields.io/github/license/alluxio/alluxio.svg)](https://github.com/Alluxio/alluxio/blob/master/LICENSE)

## What is Alluxio
[Alluxio](https://www.alluxio.org) (formerly known as Tachyon)
is a virtual distributed storage system. It bridges the gap between
computation frameworks and storage systems, enabling computation applications to connect to
numerous storage systems through a common interface. Read more about
[Alluxio Overview](https://www.alluxio.org/docs/1.8/en/Overview.html).

The Alluxio project originated from a research project called Tachyon at AMPLab, UC Berkeley,
which was the data layer of the Berkeley Data Analytics Stack ([BDAS](https://amplab.cs.berkeley.edu/bdas/)).
For more details, please refer to Haoyuan Li's PhD dissertation 
[Alluxio: A Virtual Distributed File System](https://www2.eecs.berkeley.edu/Pubs/TechRpts/2018/EECS-2018-29.html).

## Who Uses Alluxio

Alluxio is used in production to manage Petabytes of data in many leading companies, with
the largest deployment exceeding 1300 nodes. Find more use cases at
[Powered by Alluxio](https://www.alluxio.org/community/powered-by-alluxio).

## Download Alluxio

### Binary download

Prebuilt binaries are available to download at https://www.alluxio.org/download .

### Docker

Download and start an Alluxio master and a worker. More details can be found in [documentation](https://www.alluxio.org/docs/1.8/en/deploy/Running-Alluxio-On-Docker.html).

```bash
# launch a master
$ docker run -d --net=host
    -v /mnt/data:/opt/alluxio/underFSStorage
    alluxio/alluxio master
# launch a worker
$ docker run -d --net=host --shm-size=1G
    -e ALLUXIO_WORKER_MEMORY_SIZE=1G
    -v /mnt/data:/opt/alluxio/underFSStorage
    -e ALLUXIO_MASTER_HOSTNAME=localhost
    alluxio/alluxio worker
```

### MacOS Homebrew

```bash
$ brew install alluxio
```

## Quick Start

Please follow the [Guide to Get Started](http://www.alluxio.org/docs/1.8/en/Getting-Started.html)
to run a simple example with Alluxio.

## Report a Bug

To report bugs, suggest improvements, or create new feature requests, please open a [Github Issue](https://github.com/alluxio/alluxio/issues). Our previous [Alluxio JIRA system](https://alluxio.atlassian.net) has been deprecated since December 2018.

## Join the Community

Please use the following to reach members of the community:

* Slack: [alluxio-community channel](https://www.alluxio.org/slack)
* Mailing List: [alluxio-users](https://groups.google.com/forum/?fromgroups#!forum/alluxio-users)
* Meetup Group: [Bay Area Meetup](http://www.meetup.com/Alluxio)
* Twitter: [@alluxio](https://twitter.com/alluxio)

## Depend on Alluxio

For Alluxio versions 1.4 or earlier, use the `alluxio-core-client` artifact.

For Alluxio versions 1.5 or later, Alluxio provides several different client artifacts. The Alluxio
file system interface provided by the `alluxio-core-client-fs` artifact is recommended for the best
performance and access to Alluxio-specific functionality. If you want to use other interfaces,
include the appropriate client artifact. For example, `alluxio-core-client-hdfs` provides a client
implementing HDFS's file system API.

### Apache Maven
```xml
<dependency>
  <groupId>org.alluxio</groupId>
  <artifactId>alluxio-core-client-fs</artifactId>
  <version>1.8.1</version>
</dependency>
```

### SBT
```
libraryDependencies += "org.alluxio" % "alluxio-core-client-fs" % "1.8.1"
```

## Contributing

Contributions via GitHub pull requests are gladly accepted from their original author. Along with
any pull requests, please state that the contribution is your original work and that you license the
work to the project under the project's open source license. Whether or not you state this
explicitly, by submitting any copyrighted material via pull request, email, or other means you agree
to license the material under the project's open source license and warrant that you have the legal
authority to do so.
For a more detailed step-by-step guide, please read
[how to contribute to Alluxio](https://www.alluxio.org/docs/1.8/en/contributor/Contributor-Getting-Started.html).
For new contributor, please take 2 [new contributor tasks](https://github.com/Alluxio/new-contributor-tasks).

## Useful Links

- [Alluxio Project Website](https://www.alluxio.org/)
- [Alluxio Inc. Website](https://www.alluxio.com/)
- [Downloads](https://www.alluxio.org/download)
- [Releases and Notes](https://www.alluxio.org/download/releases/)
- [Documentation](https://www.alluxio.org/docs/)
