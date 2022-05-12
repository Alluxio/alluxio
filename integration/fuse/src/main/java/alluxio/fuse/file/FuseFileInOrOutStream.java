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

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.fuse.AlluxioFuseOpenUtils;
import alluxio.fuse.auth.AuthPolicy;

import jnr.constants.platform.OpenFlags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.InvalidPathException;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * An implementation for {@link FuseFileStream} for read only or write only workloads.
 * Fuse can open file for reading and writing concurrently but Alluxio only support
 * read-only or write-only workloads. This class will treat the stream as read-only or write-only
 * workloads based on whether truncate is called and the first operation is read or write.
 */
@ThreadSafe
public class FuseFileInOrOutStream implements FuseFileStream {
  private static final Logger LOG = LoggerFactory.getLogger(FuseFileInOrOutStream.class);
  private final FileSystem mFileSystem;
  private final AlluxioURI mUri;
  private final long mMode;
  private final AuthPolicy mAuthPolicy;

  // Treat as read only or write only stream based on first operation is read or truncate or write
  private FuseFileStream mStream;

  /**
   * Creates a {@link FuseFileInOrOutStream}.
   *
   * @param fileSystem the file system
   * @param uri the alluxio uri
   * @param flags the fuse create/open flags
   * @param authPolicy the authentication policy
   * @param mode the filesystem mode, -1 if not set
   * @return a {@link FuseFileInOrOutStream}
   */
  public static FuseFileInOrOutStream create(FileSystem fileSystem, AlluxioURI uri, int flags,
      AuthPolicy authPolicy, long mode) throws Exception {
    URIStatus status;
    try {
      status = fileSystem.getStatus(uri);
    } catch (InvalidPathException | FileNotFoundException | FileDoesNotExistException e) {
      status = null;
    } catch (Throwable t) {
      throw new IOException(String.format("Failed to create fuse stream for %s, "
          + "unexpected error when getting file status", uri), t);
    }
    boolean truncate = AlluxioFuseOpenUtils.containsTruncate(flags);
    if (status != null && truncate) {
      fileSystem.delete(uri);
      LOG.debug(String.format("Open path %s with flag 0x%x for overwriting. "
          + "Alluxio deleted the old file and created a new file for writing", uri, flags));
    }
    if (status == null || truncate) {
      FuseFileOutStream stream = FuseFileOutStream.create(fileSystem,
          uri, OpenFlags.O_WRONLY.intValue(), authPolicy, mode);
      return new FuseFileInOrOutStream(stream, fileSystem, uri, authPolicy, mode);
    }
    // Left for next operation to decide read-only or write-only mode
    // read-only: open(READ_WRITE) existing file - read()
    // write-only: open(READ_WRITE) existing file - truncate(0) - write()
    return new FuseFileInOrOutStream(null, fileSystem, uri, authPolicy, mode);
  }

  private FuseFileInOrOutStream(@Nullable FuseFileStream stream, FileSystem fileSystem,
      AlluxioURI uri, AuthPolicy authPolicy, long mode) {
    mStream = stream;
    mFileSystem = fileSystem;
    mUri = uri;
    mMode = mode;
    mAuthPolicy = authPolicy;
  }

  @Override
  public synchronized int read(ByteBuffer buf, long size, long offset)
      throws IOException, AlluxioException {
    if (mStream == null) {
      mStream = FuseFileInStream.create(mFileSystem, mUri, OpenFlags.O_RDONLY.intValue());
    } else if (!isRead()) {
      throw new IOException("Alluxio does not support reading while writing");
    }
    return mStream.read(buf, size, offset);
  }

  @Override
  public synchronized void write(ByteBuffer buf, long size, long offset) throws Exception {
    if (mStream == null) {
      mStream = FuseFileOutStream.create(mFileSystem, mUri, OpenFlags.O_WRONLY.intValue(),
          mAuthPolicy, mMode);
    } else if (isRead()) {
      throw new IOException("Alluxio does not support reading while writing");
    }
    mStream.write(buf, size, offset);
  }

  @Override
  public synchronized long getFileLength() throws IOException {
    if (mStream != null) {
      return mStream.getFileLength();
    }
    URIStatus status;
    try {
      status = mFileSystem.getStatus(mUri);
      return status.getLength();
    } catch (InvalidPathException | FileNotFoundException | FileDoesNotExistException e) {
      return 0;
    } catch (Throwable t) {
      throw new IOException(String
          .format("Failed to get file length of %s: failed to get file status", mUri), t);
    }
  }

  @Override
  public synchronized void flush() throws IOException {
    if (mStream != null) {
      mStream.flush();
    }
  }

  @Override
  public synchronized void truncate(long size) throws Exception {
    if (mStream != null) {
      mStream.truncate(size);
      return;
    }
    if (size == getFileLength()) {
      return;
    }
    if (size == 0) {
      mFileSystem.delete(mUri);
      mStream = FuseFileOutStream.create(mFileSystem, mUri,
          OpenFlags.O_WRONLY.intValue(), mAuthPolicy, mMode);
    }
  }

  @Override
  public synchronized void close() throws IOException {
    if (mStream != null) {
      mStream.close();
    }
  }

  private boolean isRead() {
    return mStream != null && mStream instanceof FuseFileInStream;
  }
}
