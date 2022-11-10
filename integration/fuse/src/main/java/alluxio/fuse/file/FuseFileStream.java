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
import alluxio.fuse.auth.AuthPolicy;
import alluxio.fuse.lock.FuseReadWriteLockManager;

import jnr.constants.platform.OpenFlags;

import java.nio.ByteBuffer;
import javax.annotation.concurrent.ThreadSafe;

/**
 * This interface should be implemented by all fuse file streams.
 */
public interface FuseFileStream extends AutoCloseable {

  /**
   * Reads data from the stream.
   *
   * @param buf the byte buffer to read data to
   * @param size the size to read
   * @param offset the offset of the target stream to begin reading
   * @return the bytes read
   */
  int read(ByteBuffer buf, long size, long offset);

  /**
   * Writes data to the stream.
   *
   * @param buf the byte buffer to read data from and write to the stream
   * @param size the size to write
   * @param offset the offset to write
   */
  void write(ByteBuffer buf, long size, long offset);

  /**
   * Gets the file length.
   *
   * @return file length
   */
  long getFileLength();

  /**
   * Flushes the stream.
   */
  void flush();

  /**
   * Truncates the file to the given size.
   *
   * @param size the truncate size
   */
  void truncate(long size);

  /**
   * Closes the stream.
   */
  void close();

  /**
   * Factory for {@link FuseFileInStream}.
   */
  @ThreadSafe
  class Factory {
    private final FuseReadWriteLockManager mLockManager = new FuseReadWriteLockManager();
    private final FileSystem mFileSystem;
    private final AuthPolicy mAuthPolicy;

    /**
     * Creates an instance of {@link FuseFileStream.Factory} for
     * creating fuse streams.
     *
     * @param fileSystem the file system
     * @param authPolicy the authentication policy
     */
    public Factory(FileSystem fileSystem, AuthPolicy authPolicy) {
      mFileSystem = fileSystem;
      mAuthPolicy = authPolicy;
    }

    /**
     * Factory method for creating/opening a file
     * and creating an implementation of {@link FuseFileStream}.
     *
     * @param uri the Alluxio URI
     * @param flags the create/open flags
     * @param mode the create file mode, -1 if not set
     * @return the created fuse file stream
     */
    public FuseFileStream create(
        AlluxioURI uri, int flags, long mode) {
      switch (OpenFlags.valueOf(flags & O_ACCMODE.intValue())) {
        case O_RDONLY:
          return FuseFileInStream.create(mFileSystem, mLockManager, uri);
        case O_WRONLY:
          return FuseFileOutStream.create(mFileSystem, mAuthPolicy, mLockManager, uri, flags, mode);
        default:
          return FuseFileInOrOutStream.create(mFileSystem, mAuthPolicy, mLockManager,
              uri, flags, mode);
      }
    }
  }
}
