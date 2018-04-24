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


## Building docker image with FUSE support
To build a docker image with 
[FUSE](https://www.alluxio.org/docs/master/en/Mounting-Alluxio-FS-with-FUSE.html) support, run

```bash
docker build -f Dockerfile.fuse -t alluxio-fuse .
```

You can add the same arguments supported by the non-FUSE docker file.


## Running docker image with FUSE support
There are a couple extra arguments required to run the docker image with FUSE support, for example:

```bash
docker run -e ALLUXIO_MASTER_HOSTNAME=alluxio-master --cap-add SYS_ADMIN --device /dev/fuse  alluxio-fuse [master|worker|proxy]
```

Note: running FUSE in docker requires adding 
[SYS_ADMIN capability](http://man7.org/linux/man-pages/man7/capabilities.7.html) to the container. 
This removes isolation of the container and should be used with caution.

## Extending docker image with applications
You can easily extend the docker image to include applications to run on top of Alluxio. 
In order for the application to access data from Alluxio storage mounted with FUSE, it must run
in the same container as Alluxio. For example, to run TensorFlow with Alluxio inside a docker 
container, just edit `Dockerfile.fuse` and replace 

```bash
FROM ubuntu:16.04
```

with

```bash
FROM tensorflow/tensorflow:1.3.0
```

You can then build the image with the same command for building image with FUSE support and run it.
There is a pre-built docker image with TensorFlow at 
https://hub.docker.com/r/alluxio/alluxio-tensorflow/ 
