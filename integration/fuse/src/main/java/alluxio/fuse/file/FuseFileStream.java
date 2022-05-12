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
import alluxio.exception.AlluxioException;
import alluxio.fuse.auth.AuthPolicy;

import jnr.constants.platform.OpenFlags;

import java.io.IOException;
import java.nio.ByteBuffer;
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
     * @param fileSystem the file system
     * @param uri the Alluxio URI
     * @param flags the create/open flags
     * @param policy the authentication policy
     * @param mode the create file mode, -1 if not set
     * @return the created fuse file stream
     */
    public static FuseFileStream create(FileSystem fileSystem, AlluxioURI uri,
        int flags, AuthPolicy policy, long mode) throws Exception {
      // TODO(lu) how can i avoid passing that many parameters to create in out stream
      switch (OpenFlags.valueOf(flags & O_ACCMODE.intValue())) {
        case O_RDONLY:
          return FuseFileInStream.create(fileSystem, uri, flags);
        case O_WRONLY:
          return FuseFileOutStream.create(fileSystem, uri, flags, policy, mode);
        case O_RDWR:
          return FuseFileInOrOutStream.create(fileSystem, uri, flags, policy, mode);
        default:
          // TODO(lu) what's the default createInternal flag?
          throw new IOException(String.format("Cannot create file stream with flag 0x%x. "
              + "Alluxio does not support file modification. "
              + "Cannot open directory in fuse.open().", flags));
      }
    }

    /**
     * Factory method for creating an implementation of {@link FuseFileStream}.
     *
     * @param fileSystem the file system
     * @param uri the Alluxio URI
     * @param flags the create/open flags
     * @param authPolicy the authentication policy
     * @return the created fuse file stream
     */
    public static FuseFileStream create(FileSystem fileSystem, AlluxioURI uri,
        int flags, AuthPolicy authPolicy) throws Exception {
      return create(fileSystem, uri, flags, authPolicy, FuseFileStream.MODE_NOT_SET);
    }
  }
}
