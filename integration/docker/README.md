To build the Alluxio Docker image from the default remote url, run

```bash
docker build -t alluxio .
```

To build with a local Alluxio tarball, specify the ALLUXIO_TARBALL build argument

```bash
docker build -t alluxio --build-arg ALLUXIO_TARBALL=alluxio-${version}.tar.gz .
```

The generated image expects to be run with single argument of "master", "worker", or "proxy".

```bash
docker run alluxio [master|worker|proxy]
```
