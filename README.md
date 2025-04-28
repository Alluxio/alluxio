[![logo](docs/resources/alluxio_logo.png "Alluxio")](https://www.alluxio.io)

[![Slack](https://img.shields.io/badge/slack-alluxio--community-blue.svg?logo=slack)](https://www.alluxio.io/slack)
[![Release](https://img.shields.io/github/release/alluxio/alluxio/all.svg)](https://www.alluxio.io/download)
[![Docker Pulls](https://img.shields.io/docker/pulls/alluxio/alluxio.svg)](https://hub.docker.com/r/alluxio/alluxio)
[![Documentation](https://img.shields.io/badge/docs-reference-blue.svg)](https://www.alluxio.io/docs)
[![OpenSSF Scorecard](https://api.securityscorecards.dev/projects/github.com/Alluxio/alluxio/badge)](https://api.securityscorecards.dev/projects/github.com/Alluxio/alluxio)
[![Twitter Follow](https://img.shields.io/twitter/follow/alluxio.svg?label=Follow&style=social)](https://twitter.com/intent/follow?screen_name=alluxio)
[![License](https://img.shields.io/github/license/alluxio/alluxio.svg)](https://github.com/Alluxio/alluxio/blob/master/LICENSE)

## What is Alluxio Open Source

Alluxio Open Source (formerly known as Tachyon) is a Distributed Caching Platform for large-scale data.
It bridges the gap between computation frameworks and storage systems, enabling computation applications to connect to numerous storage systems through a common interface.

This GitHub repository is the open-source edition of Alluxio, which is purpose-built for analytics workloads.
It provides caching and acceleration for structured data analytics and is widely adopted with data-intensive computation engines such as Presto, Spark, and Trino.

The Alluxio project originated from a research project called Tachyon at AMPLab, UC Berkeley,
which was the data layer of the Berkeley Data Analytics Stack ([BDAS](https://amplab.cs.berkeley.edu/bdas/)).
For more details, please refer to Haoyuan Li's PhD dissertation
[Alluxio: A Virtual Distributed File System](https://www2.eecs.berkeley.edu/Pubs/TechRpts/2018/EECS-2018-29.html).

## Alluxio Open Source versus Enterprise Editions

The open-source edition of Alluxio is purpose-built for analytics workloads.
It accelerates structured data analytics and scales to manage up to 100 million files.
This Alluxio Open Source Edition is available for free without support and is recommended for testing, development, and small-scale production environments.

For AI and machine learning workloads, including model training, distribution and inference at scale, Alluxio Enterprise Edition provides a fundamentally different architecture with a decentralized metadata service.
As a result, it scales horizontally to support tens of billions of files and delivers higher performance along with FUSE-based POSIX integration for compatibility with popular AI frameworks, including PyTorch, TensorFlow, and Ray. 

To learn more about Alluxio Enterprise Edition, visit: [https://www.alluxio.io/enterprise-ai](https://www.alluxio.io/enterprise-ai). 
For more details about the differences between editions, visit [https://www.alluxio.io/editions](https://www.alluxio.io/editions).

## Who Owns and Manages Alluxio Open Source Project

Alluxio Open Source Foundation is the owner of Alluxio project.
Project operation is done by Alluxio Project Management Committee (PMC).
You can check out more details on its structure and how to join Alluxio PMC [here](https://github.com/Alluxio/alluxio/wiki/Alluxio-Project-Management-Committee-(PMC)).

## Community and Events

Please use the following to reach members of the community:
- [Alluxio Community Slack Channel](https://www.alluxio.io/slack): post your questions here if you seek help for general questions or issues using Alluxio.
- [Community Events](https://www.alluxio.io/events): upcoming online tech talks, meetups and webinars.
- [Alluxio Twitter](https://twitter.com/alluxio); [Alluxio Youtube Channel](https://www.youtube.com/channel/UCpibQsajhwqYPLYhke4RigA); [Alluxio Linkedin](https://www.linkedin.com/company/alluxio-inc-/).

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
  [Alluxio Java file system API](https://docs.alluxio.io/os/user/stable/en/api/Java-API.html#alluxio-java-api))
  to access all Alluxio-specific functionalities.
  This artifact is included in `alluxio-shaded-client`.
- Artifact `alluxio-core-client-hdfs` provides
  [HDFS-Compatible file system API](https://docs.alluxio.io/os/user/stable/en/api/Java-API.html#hadoop-compatible-java-client).
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


## Useful Links

- [Documentation](https://documentation.alluxio.io/os-en)
- [Getting Started](https://documentation.alluxio.io/os-en/overview-1/getting-started)
- [Contribution Guide](https://documentation.alluxio.io/os-en/contributor/contributor-getting-started)
- [Alluxio Website](https://www.alluxio.io/)
