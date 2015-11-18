# Tachyon-FUSE
Mount a Tachyon DFS using FUSE. 

This project uses [jnr-fuse](https://github.com/SerCeMan/jnr-fuse) for FUSE on Java.

## Requirements
* JDK 1.8+
* SBT 0.13.9+
* Tachyon 0.8.2
* libfuse 2.9.3+
  (2.8.3 has been reported to also work - with some warnings)


## Usage

1. `$ sbt assembly`
2. Edit `bin/tachyon-fuse.sh` with your own configuration. (At least, you have to set the path to your `tachyon-client-0.8.2-jar-with-depenencies.jar`.)
3. `$ bin/tachyon-fuse.sh`

*Optional*:
Edit `bin/tachyon-fuse.sh` and add your specific tachyon client options in the
`TACHYON_JAVA_OPTS` variable.


## Status
Most basic operations are supported. However, due to Tachyon implicit characteristics, note that:
* Files can be written only once and only sequentially, and never modified.
* Due to the above, further `open` operations on the file must be O_RDONLY, or they will fail.

The project is **experimental**, so use it at your own risk.

### Performance considerations
Due to the conjunct use of FUSE and JNR, the performance of the mounted file system is expected
to be considerably worst than what you would see by using the `tachyon-client` directly.

Most of the problems
come from the fact that there are several memory copies going on for each call on `read` or
`write` operations, and that FUSE caps the maximum granularity of writes to 128KB. This could be
probably improved by a large extent by leveraging the FUSE cache write-backs feature introduced in
kernel 3.15.

