# Alluxio-FUSE (Experimental)
Mount a Alluxio file system using FUSE. 

This project uses [jnr-fuse](https://github.com/SerCeMan/jnr-fuse) for FUSE on Java.

## Requirements
* Linux kernel 2.6.9 or newer
* JDK 1.8 or newer
* libfuse 2.9.3 or newer
  (2.8.3 has been reported to also work - with some warnings)

## Building
The alluxio-integration-fuse module is automatically built with Alluxio when the `fuse` maven
profile is active. This profile is automatically activated when maven detects a JDK 8 or newer.

For compatibility, binary Alluxio distributions may ship without alluxio-fuse support. Please,
rebuild your favourite Alluxio version yourself if you want to use alluxio-fuse (see [Building
Alluxio](http://alluxio.org/documentation/master/Building-Alluxio-Master-Branch.html)).

## Usage

### Mounting
After having configured and started the Alluxio cluster:
`$ bin/alluxio-fuse.sh mount <mount_point>`

**Note**: the user running the script must own the mount point and
have rw permissions on it.

### Unmounting
`$ bin/alluxio-fuse.sh umount`

### Check if running
`$ bin/alluxio-fuse.sh stat`

### Optional
Edit `bin/alluxio-fuse.sh` and add your specific Alluxio client options in the
`ALLUXIO_JAVA_OPTS` variable.

## Status
Most basic operations are supported. However, due to Alluxio implicit characteristics, note that:
* Files can be written only once and only sequentially, and never modified.
* Due to the above, further `open` operations on the file must be O_RDONLY, or they will fail.

The project is **experimental**, so use it at your own risk.

### Performance considerations
Due to the conjunct use of FUSE and JNR, the performance of the mounted file system is expected
to be considerably worst than what you would see by using the `alluxio-core-client` directly.

Most of the problems come from the fact that there are several memory copies going on for each call
on `read` or `write` operations, and that FUSE caps the maximum granularity of writes to 128KB. This
could be probably improved by a large extent by leveraging the FUSE cache write-backs feature
introduced in kernel 3.15 (not supported yet by libfuse 2.x userspace libs).

