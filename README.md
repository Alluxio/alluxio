<div align="center">
  <img src="http://www.alluxio.org/static/media/alluxio-logo-horizontal-tight.a41c789d.svg"><br><br>
</div>

[![GitHub release](https://img.shields.io/github/release/alluxio/alluxio/all.svg?style=flat-square)](https://github.com/alluxio/alluxio/releases)
[![Docker Pulls](https://img.shields.io/docker/pulls/alluxio/alluxio.svg)](https://hub.docker.com/r/alluxio/alluxio)
[![Documentation](https://img.shields.io/badge/api-reference-blue.svg)](https://www.alluxio.org/docs)
[![Twitter Follow](https://img.shields.io/twitter/follow/alluxio.svg?label=Follow&style=social)](https://twitter.com/intent/follow?screen_name=alluxio)

Alluxio (formerly Tachyon)
=======

## Quick Start

Please follow the [Guide to Get Started](http://www.alluxio.org/docs/1.8/en/Getting-Started.html) with Alluxio.

## Download Alluxio

### Using web download

Prebuilt binaries are served at https://www.alluxio.org/download 

### Using Docker

Download and start an Alluxio master

```bash
$ docker run -d --net=host 
    -v /mnt/data:/opt/alluxio/underFSStorage 
    alluxio/alluxio master
```

Download and start an Alluxio worker

```bash
$ docker run -d --net=host --shm-size=1G 
    -e ALLUXIO_WORKER_MEMORY_SIZE=1G 
    -v /mnt/data:/opt/alluxio/underFSStorage 
    -e ALLUXIO_MASTER_HOSTNAME=localhost 
    alluxio/alluxio worker
```

### Using MacOS Brew

```bash
$ brew install alluxio
```

## Report a Bug

To file bugs, suggest improvements, or create new feature requests, please open an [Github Issue](https://github.com/alluxio/alluxio/issues). Our previous [Alluxio JIRA system](https://alluxio.atlassian.net) has been deprecated since December, 2018.


## Build applications with Alluxio

### Dependency Information

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

### Gradle

```groovy
compile 'org.alluxio:alluxio-core-client-fs:1.8.1'
```

### Apache Ant
```xml
<dependency org="org.alluxio" name="alluxio" rev="1.8.1">
  <artifact name="alluxio-core-client-fs" type="jar" />
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
For more detailed step-by-step tutorials, please read 
[Contribute to Alluxio](https://www.alluxio.org/docs/1.8/en/contributor/Contributor-Getting-Started.html). For [new contributor tasks](https://github.com/Alluxio/new-contributor-tasks), please limit 2 tasks per new contributor.

## Useful Links

- [Alluxio Open Source Website](http://www.alluxio.org/) 
- [Alluxio Inc.](http://www.alluxio.com/)
- [Alluxio Latest Release Document](https://www.alluxio.org/download/releases/)
- [Master Branch Document](https://www.alluxio.org/docs/master/en/)
- [Releases](http://alluxio.org/releases/)
- [Downloads](http://www.alluxio.org/download)
- [User Mailing List](https://groups.google.com/forum/?fromgroups#!forum/alluxio-users)
- [Bay Area Meetup Group](http://www.meetup.com/Alluxio)