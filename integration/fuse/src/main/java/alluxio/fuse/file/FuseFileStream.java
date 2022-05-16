/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.fuse.file;

import static jnr.constants.platform.OpenFlags.O_ACCMODE;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.fuse.AlluxioFuseUtils;
import alluxio.fuse.auth.AuthPolicy;

import jnr.constants.platform.OpenFlags;

import java.io.IOException;
import java.nio.ByteBuffer;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * This interface should be implemented by all fuse file streams.
 */
public interface FuseFileStream {
  long MODE_NOT_SET = -1;

  /**
   * Reads data from the stream.
   *
   * @param buf the byte buffer to read data to
   * @param size the size to read
   * @param offset the offset to read
   * @return the bytes read
   */
  int read(ByteBuffer buf, long size, long offset) throws IOException, AlluxioException;

  /**
   * Writes data to the stream.
   *
   * @param buf the byte buffer to read data from and write to the stream
   * @param size the size to write
   * @param offset the offset to write
   */
  void write(ByteBuffer buf, long size, long offset) throws Exception;

  /**
   * Gets the file length.
   *
   * @return file length
   */
  long getFileLength() throws IOException;

  /**
   * Flushes the stream.
   */
  void flush() throws IOException;

  /**
   * Truncates the file to the given size.
   *
   * @param size the truncate size
   */
  void truncate(long size) throws Exception;

  /**
   * Closes the stream.
   */
  void close() throws IOException;

  /**
   * Factory for {@link FuseFileInStream}.
   */
  @ThreadSafe
  class Factory {
    private Factory() {} // prevent instantiation

    /**
     * Factory method for creating an implementation of {@link FuseFileStream}.
     *
     * @param fileSystem the Alluxio file system
     * @param authPolicy the Authentication policy
     * @param uri the Alluxio URI
     * @param flags the create/open flags
     * @param mode the create file mode, -1 if not set
     * @param status the uri status, null if not uri does not exist
     * @return the created fuse file stream
     */
    public static FuseFileStream create(FileSystem fileSystem, AuthPolicy authPolicy,
        AlluxioURI uri, int flags, long mode, @Nullable URIStatus status) throws Exception {
      switch (OpenFlags.valueOf(flags & O_ACCMODE.intValue())) {
        case O_RDONLY:
          return FuseFileInStream.create(fileSystem, uri, flags, status);
        case O_WRONLY:
          return FuseFileOutStream.create(fileSystem, authPolicy, uri, flags, mode, status);
        case O_RDWR:
          return FuseFileInOrOutStream.create(fileSystem, authPolicy, uri, flags, mode, status);
        default:
          throw new IOException(String.format("Cannot create file stream with flag 0x%x. "
              + "Alluxio does not support file modification. "
              + "Cannot open directory in fuse.open().", flags));
      }
    }

    /**
     * Factory method for creating an implementation of {@link FuseFileStream}.
     *
     * @param fileSystem the Alluxio file system
     * @param authPolicy the Authentication policy
     * @param uri the Alluxio URI
     * @param flags the create/open flags
     * @param mode the create file mode, -1 if not set
     * @return the created fuse file stream
     */
    public static FuseFileStream create(FileSystem fileSystem, AuthPolicy authPolicy,
        AlluxioURI uri, int flags, long mode) throws Exception {
      URIStatus status = AlluxioFuseUtils.getPathStatus(fileSystem, uri);
      return create(fileSystem, authPolicy, uri, flags, mode, status);
    }

    /**
     * Factory method for creating an implementation of {@link FuseFileStream}.
     *
     * @param fileSystem the Alluxio file system
     * @param authPolicy the Authentication policy
     * @param uri the Alluxio URI
     * @param flags the create/open flags
     * @return the created fuse file stream
     */
    public static FuseFileStream create(FileSystem fileSystem, AuthPolicy authPolicy,
        AlluxioURI uri, int flags) throws Exception {
      return create(fileSystem, authPolicy, uri, flags, FuseFileStream.MODE_NOT_SET);
    }
  }
}
