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
import alluxio.fuse.AlluxioFuseOpenUtils;
import alluxio.fuse.auth.AuthPolicy;

import com.google.common.base.Preconditions;
import jnr.constants.platform.OpenFlags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * An implementation for {@link FuseFileStream} for read only or write only workloads.
 * Fuse can open file for reading and writing concurrently but Alluxio only support
 * read-only or write-only workloads. This class will be used as write only stream
 * if O_TRUNC flag is provided or first operation is write() or truncate(),
 * will be used as read only stream otherwise.
 */
@ThreadSafe
public class FuseFileInOrOutStream implements FuseFileStream {
  private static final Logger LOG = LoggerFactory.getLogger(FuseFileInOrOutStream.class);
  private final FileSystem mFileSystem;
  private final AuthPolicy mAuthPolicy;
  private final AlluxioURI mUri;
  private final long mMode;
  private final Optional<URIStatus> mStatus;

  // underlying reed-only or write-only stream
  private FuseFileStream mStream;

  /**
   * Creates a {@link FuseFileInOrOutStream}.
   *
   * @param fileSystem the Alluxio file system
   * @param authPolicy the Authentication policy
   * @param uri the alluxio uri
   * @param flags the fuse create/open flags
   * @param mode the filesystem mode, -1 if not set
   * @param status the uri status, null if not uri does not exist
   * @return a {@link FuseFileInOrOutStream}
   */
  public static FuseFileInOrOutStream create(FileSystem fileSystem, AuthPolicy authPolicy,
      AlluxioURI uri, int flags, long mode, Optional<URIStatus> status)
      throws IOException, ExecutionException, AlluxioException {
    Preconditions.checkNotNull(fileSystem);
    Preconditions.checkNotNull(uri);
    Preconditions.checkNotNull(status);
    boolean truncate = AlluxioFuseOpenUtils.containsTruncate(flags);
    if (status.isPresent() && truncate) {
      fileSystem.delete(uri);
      status = Optional.empty();
      LOG.debug(String.format("Open path %s with flag 0x%x for overwriting. "
          + "Alluxio deleted the old file and created a new file for writing", uri, flags));
    }
    if (!status.isPresent()) {
      FuseFileOutStream stream = FuseFileOutStream.create(fileSystem, authPolicy,
          uri, OpenFlags.O_WRONLY.intValue(), mode, status);
      return new FuseFileInOrOutStream(fileSystem, authPolicy, stream, status, uri, mode);
    }
    // Left for next operation to decide read-only or write-only mode
    // read-only: open(READ_WRITE) existing file - read()
    // write-only: open(READ_WRITE) existing file - truncate(0) - write()
    return new FuseFileInOrOutStream(fileSystem, authPolicy, null, status, uri, mode);
  }

  private FuseFileInOrOutStream(FileSystem fileSystem, AuthPolicy authPolicy,
      @Nullable FuseFileStream stream, Optional<URIStatus> status, AlluxioURI uri, long mode) {
    mFileSystem = fileSystem;
    mAuthPolicy = authPolicy;
    mStream = stream;
    mStatus = status;
    mUri = uri;
    mMode = mode;
  }

  @Override
  public synchronized int read(ByteBuffer buf, long size, long offset)
      throws IOException, AlluxioException {
    if (mStream == null) {
      mStream = FuseFileInStream.create(mFileSystem, mUri,
          OpenFlags.O_RDONLY.intValue(), mStatus);
    } else if (!isRead()) {
      throw new UnsupportedOperationException(
          "Alluxio does not support reading while writing/truncating");
    }
    return mStream.read(buf, size, offset);
  }

  @Override
  public synchronized void write(ByteBuffer buf, long size, long offset)
      throws IOException, ExecutionException, AlluxioException {
    if (mStream == null) {
      mStream = FuseFileOutStream.create(mFileSystem, mAuthPolicy, mUri,
          OpenFlags.O_WRONLY.intValue(), mMode, mStatus);
    } else if (isRead()) {
      throw new UnsupportedOperationException(
          "Alluxio does not support reading while writing/truncating");
    }
    mStream.write(buf, size, offset);
  }

  @Override
  public synchronized long getFileLength() throws IOException {
    if (mStream != null) {
      return mStream.getFileLength();
    }
    if (mStatus.isPresent()) {
      return mStatus.get().getLength();
    }
    return 0;
  }

  @Override
  public synchronized void flush() throws IOException {
    if (mStream != null) {
      mStream.flush();
    }
  }

  @Override
  public synchronized void truncate(long size)
      throws IOException, ExecutionException, AlluxioException {
    if (mStream != null) {
      mStream.truncate(size);
      return;
    }
    if (size == getFileLength()) {
      return;
    }
    if (size == 0) {
      mFileSystem.delete(mUri);
      mStream = FuseFileOutStream.create(mFileSystem, mAuthPolicy, mUri,
          OpenFlags.O_WRONLY.intValue(), mMode, null);
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
