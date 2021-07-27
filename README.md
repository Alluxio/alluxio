[![logo](docs/resources/alluxio_logo.png "Alluxio")](https://www.alluxio.io)

[![Slack](https://img.shields.io/badge/slack-alluxio--community-blue.svg?logo=slack)](https://www.alluxio.io/slack)
[![Release](https://img.shields.io/github/release/alluxio/alluxio/all.svg)](https://www.alluxio.io/download)
[![Docker Pulls](https://img.shields.io/docker/pulls/alluxio/alluxio.svg)](https://hub.docker.com/r/alluxio/alluxio)
[![Documentation](https://img.shields.io/badge/docs-reference-blue.svg)](https://www.alluxio.io/docs)
[![Twitter Follow](https://img.shields.io/twitter/follow/alluxio.svg?label=Follow&style=social)](https://twitter.com/intent/follow?screen_name=alluxio)
[![License](https://img.shields.io/github/license/alluxio/alluxio.svg)](https://github.com/Alluxio/alluxio/blob/master/LICENSE)

## What is Alluxio
[Alluxio](https://www.alluxio.io) (formerly known as Tachyon)
is a virtual distributed storage system. It bridges the gap between
computation frameworks and storage systems, enabling computation applications to connect to
numerous storage systems through a common interface. Read more about
[Alluxio Overview](https://docs.alluxio.io/os/user/stable/en/Overview.html).

The Alluxio project originated from a research project called Tachyon at AMPLab, UC Berkeley,
which was the data layer of the Berkeley Data Analytics Stack ([BDAS](https://amplab.cs.berkeley.edu/bdas/)).
For more details, please refer to Haoyuan Li's PhD dissertation
[Alluxio: A Virtual Distributed File System](https://www2.eecs.berkeley.edu/Pubs/TechRpts/2018/EECS-2018-29.html).

## Who Uses Alluxio

Alluxio is used in production to manage Petabytes of data in many leading companies, with
the largest deployment exceeding 3,000 nodes. You can find more use cases at
[Powered by Alluxio](https://www.alluxio.io/powered-by-alluxio) or visit our first community conference ([Data Orchestration Summit](https://www.alluxio.io/data-orchestration-summit-2019/)) to learn from other community members!

## Who Owns and Manages Alluxio Project

Alluxio Open Source Foundation is the owner of Alluxio project.
Project operation is done by Alluxio Project Management Committee (PMC).
You can checkout more details in its structure and how to join Alluxio PMC 
[here](https://github.com/Alluxio/alluxio/wiki/Alluxio-Project-Management-Committee-(PMC)).

## Community and Events
Please use the following to reach members of the community:

* [Alluxio Community Slack Channel](https://www.alluxio.io/slack): post your questions here if you seek for help for general questions or issues using Alluxio.
* [Special Interest Groups (SIG) for Alluxio users and developers](#contributing)
* Community Events: [upcoming online office hours, meetups and webinars](https://www.alluxio.io/events)
* Meetup Groups: [Global Online Meetup](https://www.meetup.com/Alluxio-Global-Online-Meetup/), [Bay Area Meetup](http://www.meetup.com/Alluxio),
[New York Meetup](https://www.meetup.com/Alluxio-Open-Source-New-York-Meetup),
[Beijing Alluxio Meetup](https://www.meetup.com/meetup-group-iLMBZGhS/), [Austin Meetup](https://www.meetup.com/Cloud-Data-Orchestration-Austin/)
* [Alluxio Twitter](https://twitter.com/alluxio); [Alluxio Youtube Channel](https://www.youtube.com/channel/UCpibQsajhwqYPLYhke4RigA); [Alluxio Mailing List](https://groups.google.com/forum/?fromgroups#!forum/alluxio-users)

## Download Alluxio

### Binary download

Prebuilt binaries are available to download at https://www.alluxio.io/download .

### Docker

Download and start an Alluxio master and a worker. More details can be found in [documentation](https://docs.alluxio.io/os/user/stable/en/deploy/Running-Alluxio-On-Docker.html).

```console
# Create a network for connecting Alluxio containers
$ docker network create alluxio_nw
# Create a volume for storing ufs data
$ docker volume create ufs
# Launch the Alluxio master
$ docker run -d --net=alluxio_nw \
    -p 19999:19999 \
    --name=alluxio-master \
    -v ufs:/opt/alluxio/underFSStorage \
    alluxio/alluxio master
# Launch the Alluxio worker
$ export ALLUXIO_WORKER_RAMDISK_SIZE=1G
$ docker run -d --net=alluxio_nw \
    --shm-size=${ALLUXIO_WORKER_RAMDISK_SIZE} \
    --name=alluxio-worker \
    -v ufs:/opt/alluxio/underFSStorage \
    -e ALLUXIO_JAVA_OPTS="-Dalluxio.worker.ramdisk.size=${ALLUXIO_WORKER_RAMDISK_SIZE} -Dalluxio.master.hostname=alluxio-master" \
    alluxio/alluxio worker
```

### MacOS Homebrew

```console
$ brew install alluxio
```

## Quick Start

Please follow the [Guide to Get Started](https://docs.alluxio.io/os/user/stable/en/Getting-Started.html)
to run a simple example with Alluxio.

## Report a Bug

To report bugs, suggest improvements, or create new feature requests, please open a [Github Issue](https://github.com/alluxio/alluxio/issues).
If you are not sure whether you run into bugs or simply have general questions with respect to Alluxio, post your questions on [Alluxio Slack channel](www.alluxio.io/slack).

## Depend on Alluxio

Alluxio project provides several different client artifacts for external projects to depend on Alluxio client:

- Artifact `alluxio-shaded-client` is recommended generally for a project to use Alluxio client.
  The jar of this artifact is self-contained (including all dependencies in a shaded form to prevent dependency conflicts),
  and thus larger than the following two artifacts.
- Artifact `alluxio-core-client-fs` provides
  [Alluxio Java file system API](https://docs.alluxio.io/os/user/stable/en/api/FS-API.html#alluxio-java-api))
  to access all Alluxio-specific functionalities.
  This artifact is included in `alluxio-shaded-client`.
- Artifact `alluxio-core-client-hdfs` provides
  [HDFS-Compatible file system API](https://docs.alluxio.io/os/user/stable/en/api/FS-API.html#hadoop-compatible-java-client).
  This artifact is included in `alluxio-shaded-client`.

Here are examples to declare the dependecies on  `alluxio-shaded-client` using Maven:

  ```xml
  <dependency>
    <groupId>org.alluxio</groupId>
    <artifactId>alluxio-shaded-client</artifactId>
    <version>2.6.0</version>
  </dependency>
  ```

## Contributing

Contributions via GitHub pull requests are gladly accepted from their original author. Along with
any pull requests, please state that the contribution is your original work and that you license the
work to the project under the project's open source license. Whether or not you state this
explicitly, by submitting any copyrighted material via pull request, email, or other means you agree
to license the material under the project's open source license and warrant that you have the legal
authority to do so.
For a more detailed step-by-step guide, please read
[how to contribute to Alluxio](https://docs.alluxio.io/os/user/stable/en/contributor/Contributor-Getting-Started.html).
For new contributor, please take two [new contributor tasks](https://github.com/Alluxio/new-contributor-tasks).

For advanced feature requests and contributions, 
Alluxio core team is hosting regular online meetings with community users and developers to iterate the project in two special interest groups:

* Alluxio and AI workloads: e.g., running Tensorflow, Pytorch on Alluxio through the POSIX API. Checkout the [meeting notes](https://docs.google.com/spreadsheets/d/1OlprIiUkGjMuZJ_6cLTJYVJpTGpnTWkFhHzX16tYNDQ/)
* Alluxio and Presto workloads: e.g., running Presto on Alluxio, running Alluxio catalog service. Checkout the [meeting notes](https://docs.google.com/spreadsheets/d/1V-fxqfG_oj3B1ZWSgbRWVuTHFvjL3pq6uXgAL-xvFQA/)

Subscribe our [public calendar](https://calendar.google.com/calendar/embed?src=alluxio.com_g9ec8agk27baqu2nu692ft1m3s%40group.calendar.google.com&ctz=America%2FLos_Angeles) to join us.

## Useful Links

- [Alluxio Website](https://www.alluxio.io/)
- [Downloads](https://www.alluxio.io/download)
- [Releases and Notes](https://www.alluxio.io/download/releases/)
- [Documentation](https://www.alluxio.io/docs/)
