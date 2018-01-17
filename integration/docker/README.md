## Building docker image
To build the Alluxio Docker image from the default remote url, run

```bash
docker build -t alluxio .
```

To build with a local Alluxio tarball, specify the `ALLUXIO_TARBALL` build argument

```bash
docker build -t alluxio --build-arg ALLUXIO_TARBALL=alluxio-${version}.tar.gz .
```

## Running docker image
The generated image expects to be run with single argument of "master", "worker", or "proxy".
To set an Alluxio configuration property, convert it to an environment variable by uppercasing
and replacing periods with underscores. For example, `alluxio.master.hostname` converts to
`ALLUXIO_MASTER_HOSTNAME`. You can then set the environment variable on the image with
`-e PROPERTY=value`. Alluxio configuration values will be copied to `conf/alluxio-site.properties`
when the image starts.

```bash
docker run -e ALLUXIO_MASTER_HOSTNAME=ec2-203-0-113-25.compute-1.amazonaws.com alluxio [master|worker|proxy]
```

Additional configuration files can be included when building the image by adding them to the
`integration/docker/conf/` directory. All contents of this directory will be
copied to `/opt/alluxio/conf`.


## Building docker image with fuse support
To build a docker image with fuse support, run

```bash
docker build -f Dockerfile.fuse -t alluxio-fuse .
```

You can add the same arguments supported by the non-fuse docker file.


## Running docker image with fuse support
There are a couple extra arguments required to run the docker image with fuse support, for example:

```bash
docker run -e ALLUXIO_MASTER_HOSTNAME=alluxio-master --cap-add SYS_ADMIN --device /dev/fuse  alluxio-fuse [master|worker|proxy]
```

## Extending docker image with applications
You can easily extend the docker image to include applications to run on top of Alluxio. 
For example, to run TensorFlow with Alluxio inside a docker container, just edit `Dockerfile.fuse`
and replace 

```bash
FROM ubuntu:16.04
```

with

```bash
FROM tensorflow/tensorflow:1.3.0
```

You can then build the image with the same command for building image with fuse support and run it.
