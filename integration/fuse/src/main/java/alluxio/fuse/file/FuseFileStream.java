package alluxio.fuse.file;

import static jnr.constants.platform.OpenFlags.O_ACCMODE;
import static jnr.constants.platform.OpenFlags.O_RDONLY;

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.exception.FileDoesNotExistException;
import alluxio.fuse.AlluxioFuseOpenUtils;
import alluxio.fuse.AlluxioFuseUtils;
import alluxio.fuse.AlluxioJniFuseFileSystem;
import alluxio.fuse.auth.AuthPolicy;
import alluxio.jnifuse.ErrorCodes;

import jnr.constants.platform.OpenFlags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.InvalidPathException;

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
    private static final Logger LOG = LoggerFactory.getLogger(FuseFileStream.Factory.class);

    private Factory() {} // prevent instantiation

    /**
     * Factory for {@link FuseFileStream}.
     */
    public static FuseFileStream create(FileSystem fileSystem, AlluxioURI uri, int flags, long mode, @Nullable AuthPolicy policy) throws Exception {
      // TODO(lu) how can i avoid passing that many parameters to create in out stream
      switch (OpenFlags.valueOf(flags & O_ACCMODE.intValue())) {
        case O_RDONLY:
          return FuseFileInStream.create(fileSystem, uri, flags);
        case O_WRONLY:
          return FuseFileOutStream.create(fileSystem, uri, flags, mode, policy);
        case O_RDWR:
          return new FuseFileInOrOutStream(fileSystem, uri, flags, mode);
        default:
          // TODO(lu) what's the default createInternal flag?
          throw new IOException(String.format("Cannot create file stream with flag 0x%x. "
              + "Alluxio does not support file modification. "
              + "Cannot open directory in fuse.open().", flags));
      }
    }

    public static FuseFileStream create(FileSystem fileSystem, AlluxioURI uri, int flags) throws Exception {
      return create(fileSystem, uri, flags, -1, null);
    }
  }
}
