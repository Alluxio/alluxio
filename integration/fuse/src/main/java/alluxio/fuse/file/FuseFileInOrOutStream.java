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
import alluxio.exception.runtime.UnimplementedRuntimeException;
import alluxio.fuse.AlluxioFuseOpenUtils;
import alluxio.fuse.AlluxioFuseUtils;
import alluxio.fuse.auth.AuthPolicy;

import com.google.common.base.Preconditions;
import jnr.constants.platform.OpenFlags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Optional;
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
  private final Optional<URIStatus> mOriginalStatus;

  // underlying reed-only or write-only stream
  // only one of them should exist
  private Optional<FuseFileInStream> mInStream;
  private Optional<FuseFileOutStream> mOutStream;

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
      AlluxioURI uri, int flags, long mode, Optional<URIStatus> status) {
    Preconditions.checkNotNull(fileSystem);
    Preconditions.checkNotNull(uri);
    Preconditions.checkNotNull(status);
    boolean truncate = AlluxioFuseOpenUtils.containsTruncate(flags);
    Optional<URIStatus> currentStatus = status;
    if (status.isPresent() && truncate) {
      AlluxioFuseUtils.deletePath(fileSystem, uri);
      currentStatus = Optional.empty();
      if (LOG.isDebugEnabled()) {
        LOG.debug(String.format("Open path %s with flag 0x%x for overwriting. "
            + "Alluxio deleted the old file and created a new file for writing", uri, flags));
      }
    }
    if (!currentStatus.isPresent()) {
      FuseFileOutStream outStream = FuseFileOutStream.create(fileSystem, authPolicy,
          uri, OpenFlags.O_WRONLY.intValue(), mode, currentStatus);
      return new FuseFileInOrOutStream(fileSystem, authPolicy,
          Optional.empty(), Optional.of(outStream), currentStatus, uri, mode);
    }
    // Left for next operation to decide read-only or write-only mode
    // read-only: open(READ_WRITE) existing file - read()
    // write-only: open(READ_WRITE) existing file - truncate(0) - write()
    return new FuseFileInOrOutStream(fileSystem, authPolicy,
        Optional.empty(), Optional.empty(), currentStatus, uri, mode);
  }

  private FuseFileInOrOutStream(FileSystem fileSystem, AuthPolicy authPolicy,
      Optional<FuseFileInStream> inStream, Optional<FuseFileOutStream> outStream,
      Optional<URIStatus> originalStatus, AlluxioURI uri, long mode) {
    mFileSystem = Preconditions.checkNotNull(fileSystem);
    mAuthPolicy = Preconditions.checkNotNull(authPolicy);
    mOriginalStatus = Preconditions.checkNotNull(originalStatus);
    mUri = Preconditions.checkNotNull(uri);
    Preconditions.checkArgument(!(inStream.isPresent() && outStream.isPresent()),
        "Cannot create both input and output stream");
    mInStream = inStream;
    mOutStream = outStream;
    mMode = mode;
  }

  @Override
  public synchronized int read(ByteBuffer buf, long size, long offset) {
    if (mOutStream.isPresent()) {
      throw new UnimplementedRuntimeException(
          "Alluxio does not support reading while writing/truncating");
    }
    if (!mInStream.isPresent()) {
      mInStream = Optional.of(FuseFileInStream.create(mFileSystem, mUri,
          mOriginalStatus));
    }
    return mInStream.get().read(buf, size, offset);
  }

  @Override
  public synchronized void write(ByteBuffer buf, long size, long offset) {
    if (mInStream.isPresent()) {
      throw new UnimplementedRuntimeException(
          "Alluxio does not support reading while writing/truncating");
    }
    if (!mOutStream.isPresent()) {
      mOutStream = Optional.of(FuseFileOutStream.create(mFileSystem, mAuthPolicy, mUri,
          OpenFlags.O_WRONLY.intValue(), mMode, mOriginalStatus));
    }
    mOutStream.get().write(buf, size, offset);
  }

  @Override
  public synchronized long getFileLength() {
    if (mOutStream.isPresent()) {
      return mOutStream.get().getFileLength();
    }
    if (mInStream.isPresent()) {
      return mInStream.get().getFileLength();
    }
    if (mOriginalStatus.isPresent()) {
      return mOriginalStatus.get().getLength();
    }
    return 0L;
  }

  @Override
  public synchronized void flush() {
    if (mInStream.isPresent()) {
      mInStream.get().flush();
      return;
    }
    mOutStream.ifPresent(FuseFileOutStream::flush);
  }

  @Override
  public synchronized void truncate(long size) {
    if (mInStream.isPresent()) {
      throw new UnimplementedRuntimeException(
          "Alluxio does not support reading while writing/truncating");
    }
    if (mOutStream.isPresent()) {
      mOutStream.get().truncate(size);
      return;
    }
    long currentSize = getFileLength();
    if (size == currentSize) {
      return;
    }
    if (size == 0 || currentSize == 0) {
      AlluxioFuseUtils.deletePath(mFileSystem, mUri);
      mOutStream = Optional.of(FuseFileOutStream.create(mFileSystem, mAuthPolicy, mUri,
          OpenFlags.O_WRONLY.intValue(), mMode, Optional.empty()));
      if (currentSize == 0) {
        mOutStream.get().truncate(size);
      }
      return;
    }
    throw new UnimplementedRuntimeException(
        String.format("Cannot truncate file %s from size %s to size %s", mUri, currentSize, size));
  }

  @Override
  public synchronized void close() {
    if (mInStream.isPresent()) {
      mInStream.get().close();
      return;
    }
    mOutStream.ifPresent(FuseFileOutStream::close);
  }
}
