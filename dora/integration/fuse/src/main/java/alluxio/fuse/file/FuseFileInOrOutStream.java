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
import alluxio.fuse.lock.FuseReadWriteLockManager;

import com.google.common.base.Preconditions;
import jnr.constants.platform.OpenFlags;

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
  private final AuthPolicy mAuthPolicy;
  private final FileSystem mFileSystem;
  private final FuseReadWriteLockManager mLockManager;
  private final long mMode;
  private final AlluxioURI mUri;

  // underlying reed-only or write-only stream
  // only one of them should exist
  private volatile Optional<FuseFileInStream> mInStream = Optional.empty();
  private Optional<FuseFileOutStream> mOutStream;
  private volatile boolean mClosed = false;

  /**
   * Creates a {@link FuseFileInOrOutStream}.
   *
   * @param fileSystem the Alluxio file system
   * @param authPolicy the Authentication policy
   * @param lockManager the lock manager
   * @param uri the alluxio uri
   * @param flags the fuse create/open flags
   * @param mode the filesystem mode, -1 if not set
   * @return a {@link FuseFileInOrOutStream}
   */
  public static FuseFileInOrOutStream create(FileSystem fileSystem, AuthPolicy authPolicy,
      FuseReadWriteLockManager lockManager, AlluxioURI uri, int flags, long mode) {
    Preconditions.checkNotNull(fileSystem);
    Preconditions.checkNotNull(lockManager);
    Preconditions.checkNotNull(uri);
    // Left for first operation to decide read-only or write-only mode
    // read-only: open(READ_WRITE) existing file - read()
    // write-only: open(READ_WRITE) existing file - truncate(0) - write()
    // write-only: open(READ_WRITE) existing file & truncate flag - write()
    // write-only: open(READ_WRITE) & O_CREAT flag - write()
    Optional<FuseFileOutStream> outStream = Optional.empty();
    if (AlluxioFuseOpenUtils.containsTruncate(flags)
        || AlluxioFuseOpenUtils.containsCreate(flags)) {
      outStream = Optional.of(FuseFileOutStream.create(fileSystem, authPolicy, lockManager,
          uri, flags, mode));
    }
    return new FuseFileInOrOutStream(fileSystem, authPolicy, lockManager, outStream, uri, mode);
  }

  private FuseFileInOrOutStream(FileSystem fileSystem, AuthPolicy authPolicy,
      FuseReadWriteLockManager lockManager, Optional<FuseFileOutStream> outStream,
      AlluxioURI uri, long mode) {
    mAuthPolicy = Preconditions.checkNotNull(authPolicy);
    mFileSystem = Preconditions.checkNotNull(fileSystem);
    mOutStream = Preconditions.checkNotNull(outStream);
    mLockManager = Preconditions.checkNotNull(lockManager);
    mUri = Preconditions.checkNotNull(uri);
    mMode = mode;
  }

  @Override
  public synchronized int read(ByteBuffer buf, long size, long offset) {
    if (mOutStream.isPresent()) {
      throw new UnimplementedRuntimeException(
          "Alluxio does not support reading while writing/truncating");
    }
    if (!mInStream.isPresent()) {
      mInStream = Optional.of(FuseFileInStream.create(mFileSystem, mLockManager, mUri));
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
      mOutStream = Optional.of(FuseFileOutStream.create(mFileSystem, mAuthPolicy,
          mLockManager, mUri, OpenFlags.O_WRONLY.intValue(), mMode));
    }
    mOutStream.get().write(buf, size, offset);
  }

  @Override
  public synchronized FileStatus getFileStatus() {
    if (mOutStream.isPresent()) {
      return mOutStream.get().getFileStatus();
    }
    if (mInStream.isPresent()) {
      return mInStream.get().getFileStatus();
    }
    return new FileStatus(AlluxioFuseUtils.getPathStatus(mFileSystem, mUri)
        .map(URIStatus::getLength).orElse(0L));
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
    if (!mOutStream.isPresent()) {
      mOutStream = Optional.of(FuseFileOutStream.create(mFileSystem, mAuthPolicy,
          mLockManager, mUri, OpenFlags.O_WRONLY.intValue(), mMode));
    }
    mOutStream.get().truncate(size);
  }

  @Override
  public synchronized void close() {
    try {
      closeStream();
    } finally {
      releaseLock();
    }
  }

  @Override
  public synchronized void releaseLock() {
    if (mInStream.isPresent()) {
      mInStream.get().releaseLock();
      return;
    }
    mOutStream.ifPresent(FuseFileOutStream::releaseLock);
  }

  @Override
  public synchronized void closeStream() {
    if (mClosed) {
      return;
    }
    mClosed = true;
    if (mInStream.isPresent()) {
      mInStream.get().closeStream();
      return;
    }
    mOutStream.ifPresent(FuseFileOutStream::closeStream);
  }

  @Override
  public boolean isClosed() {
    return mClosed;
  }

  @Override
  public boolean isReadOnly() {
    // The inner in stream and out stream are mutual exclusive.
    // Once the stream is used as an in stream, it cannot be
    // used as an out stream anymore and hence it is read only.
    return mInStream.isPresent();
  }
}
