This is the docker image of the Hive Metastore, for integration tests. For some integration
tests which use [Testcontainers](https://www.testcontainers.org/), this docker image can be used
to launch a local Hive Metastore.

## Building docker image
To build the docker image , run

```console
$ docker build -t hms:latest .
```

## Running tests with this docker image
Once this image is built, it can be referenced and used by integration tests, using the
Testcontainers framework.
