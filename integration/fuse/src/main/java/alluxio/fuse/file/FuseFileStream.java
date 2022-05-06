package alluxio.fuse.file;

import static jnr.constants.platform.OpenFlags.O_ACCMODE;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.fuse.auth.AuthPolicy;

import jnr.constants.platform.OpenFlags;

import java.io.IOException;
import java.nio.ByteBuffer;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

public interface FuseFileStream {
  int read(ByteBuffer buf, long size, long offset) throws IOException;
  int write(ByteBuffer buf, long size, long offset) throws IOException;
  int getFileLength() throws IOException;
  int flush() throws IOException;
  int truncate(long size) throws IOException;
  int close() throws IOException;
  

  /**
   * Factory for {@link FuseFileInStream}.
   */
  @ThreadSafe
  class Factory {

    private Factory() {} // prevent instantiation

    /**
     * Factory for {@link FuseFileStream}.
     */
    public static FuseFileStream create(FileSystem fileSystem, AlluxioURI uri, int flags, long mode, @Nullable AuthPolicy policy) throws IOException {
      switch (OpenFlags.valueOf(flags & O_ACCMODE.intValue())) {
        case O_RDONLY:
          return new FuseFileInStream(fileSystem, uri);
        case O_WRONLY:
          return FuseFileOutStream.create(fileSystem, uri, mode, policy);
        case O_RDWR:
          return new FuseFileInOrOutStream(fileSystem, uri, mode);
        default:
          // TODO(lu) what's the default createInternal flag?
          throw new IOException(String.format("Cannot create file stream with flag %s", flags));
      }
    }

    public static FuseFileStream create(FileSystem fileSystem, AlluxioURI uri, int flags) throws IOException {
      return create(fileSystem, uri, flags, -1, null);
    }
  }
}
