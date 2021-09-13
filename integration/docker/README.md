## Prerequisite
To build your Alluxio Docker image, a Docker 19.03+ is required. 

## Building docker image for production
To build the Alluxio Docker image from the default remote url, run
```console
$ docker build -t alluxio/alluxio .
```

To build with a local Alluxio tarball, specify the `ALLUXIO_TARBALL` build argument

```console
$ docker build -t alluxio/alluxio --build-arg ALLUXIO_TARBALL=alluxio-${version}.tar.gz .
```

Starting from v2.6.0, alluxio-fuse image is deprecated. It is embedded in alluxio/alluxio image.

## Building docker image for development
Starting from now, Alluxio has a separate image for development usage. Unlike the default Alluxio 
Docker image that only installs packages needed for Alluxio service to run, this image installs 
more development tools, including gcc, make, async-profiler, etc., making it easier to deploy more 
services along with Alluxio.

To build the development image from the default remote url, run
```console
$ docker build -t alluxio/alluxio-dev -f Dockerfile-dev .
```

To build with a local Alluxio tarball, specify the `ALLUXIO_TARBALL` build argument

```console
$ docker build -t alluxio/alluxio-dev -f Dockerfile-dev \
--build-arg ALLUXIO_TARBALL=alluxio-${version}.tar.gz .
```

## Running docker image
The generated image expects to be run with single argument of "master", "worker", "proxy", or "fuse".
To set an Alluxio configuration property, convert it to an environment variable by uppercasing
and replacing periods with underscores. For example, `alluxio.master.hostname` converts to
`ALLUXIO_MASTER_HOSTNAME`. You can then set the environment variable on the image with
`-e PROPERTY=value`. Alluxio configuration values will be copied to `conf/alluxio-site.properties`
when the image starts.

```console
$ docker run -e ALLUXIO_MASTER_HOSTNAME=ec2-203-0-113-25.compute-1.amazonaws.com \
alluxio/alluxio-[
|dev] [master|worker|proxy|fuse]
```

Additional configuration files can be included when building the image by adding them to the
`integration/docker/conf/` directory. All contents of this directory will be
copied to `/opt/alluxio/conf`.

## Running docker image with FUSE support
There are a couple extra arguments required to run the docker image with FUSE support. For example,
to launch a standalone Fuse container:

```console
$ docker run -e ALLUXIO_MASTER_HOSTNAME=alluxio-master \
--cap-add SYS_ADMIN --device /dev/fuse alluxio/alluxio fuse --fuse-opts=allow_other
```

Note: running FUSE in docker requires adding
[SYS_ADMIN capability](http://man7.org/linux/man-pages/man7/capabilities.7.html) to the container.
This removes isolation of the container and should be used with caution.

## Extending docker image with applications
You can easily extend the docker image to include applications to run on top of Alluxio.
In order for the application to access data from Alluxio storage mounted with FUSE, it must run
in the same container as Alluxio FUSE. Simply edit `Dockerfile` to install the applications, and 
then build the image with the same command for building image with FUSE support and run it.

## More information
For more information on launching alluxio in docker containers, please refer to
https://docs.alluxio.io/os/user/stable/en/deploy/Running-Alluxio-On-Docker.html
