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

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;
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
  private final FileSystem mFileSystem;
  private final AuthPolicy mAuthPolicy;
  private final AlluxioURI mUri;
  private final long mMode;
  private final ReadWriteLock mLock;

  // underlying reed-only or write-only stream
  // only one of them should exist
  private Optional<FuseFileInStream> mInStream = Optional.empty();
  private Optional<FuseFileOutStream> mOutStream;

  /**
   * Creates a {@link FuseFileInOrOutStream}.
   *
   * @param fileSystem the Alluxio file system
   * @param authPolicy the Authentication policy
   * @param uri the alluxio uri
   * @param lock the path lock
   * @param flags the fuse create/open flags
   * @param mode the filesystem mode, -1 if not set
   * @return a {@link FuseFileInOrOutStream}
   */
  public static FuseFileInOrOutStream create(FileSystem fileSystem, AuthPolicy authPolicy,
      AlluxioURI uri, ReadWriteLock lock, int flags, long mode) {
    Preconditions.checkNotNull(fileSystem);
    Preconditions.checkNotNull(uri);
    // Left for first operation to decide read-only or write-only mode
    // read-only: open(READ_WRITE) existing file - read()
    // write-only: open(READ_WRITE) existing file - truncate(0) - write()
    // write-only: open(READ_WRITE) existing file & truncate flag - write()
    Optional<FuseFileOutStream> outStream = Optional.empty();
    if (AlluxioFuseOpenUtils.containsTruncate(flags)
        || AlluxioFuseOpenUtils.containsCreate(flags)) {
      outStream = Optional.of(FuseFileOutStream.create(fileSystem, authPolicy,
          uri, lock, flags, mode));
    }
    return new FuseFileInOrOutStream(fileSystem, authPolicy, outStream, lock, uri, mode);
  }

  private FuseFileInOrOutStream(FileSystem fileSystem, AuthPolicy authPolicy,
      Optional<FuseFileOutStream> outStream, ReadWriteLock lock, AlluxioURI uri, long mode) {
    mFileSystem = Preconditions.checkNotNull(fileSystem);
    mAuthPolicy = Preconditions.checkNotNull(authPolicy);
    mOutStream = Preconditions.checkNotNull(outStream);
    mLock = Preconditions.checkNotNull(lock);
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
      mInStream = Optional.of(FuseFileInStream.create(mFileSystem, mUri,
          mLock));
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
          mUri, mLock, OpenFlags.O_WRONLY.intValue(), mMode));
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
    return AlluxioFuseUtils.getPathStatus(mFileSystem, mUri)
        .map(URIStatus::getLength).orElse(0L);
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
      mOutStream = Optional.of(FuseFileOutStream.create(mFileSystem, mAuthPolicy, mUri, mLock,
          OpenFlags.O_WRONLY.intValue(), mMode));
    }
<<<<<<< HEAD
    mOutStream.get().truncate(size);
||||||| 11c1c7c5bf
    throw new UnsupportedOperationException(
        String.format("Cannot truncate file %s from size %s to size %s", mUri, currentSize, size));
=======
    throw new UnimplementedRuntimeException(
        String.format("Cannot truncate file %s from size %s to size %s", mUri, currentSize, size));
>>>>>>> 4eddd3e9fa3cb7c13d4b04004bb732499b586890
  }

  @Override
  public synchronized void close() {
    if (mInStream.isPresent()) {
      mInStream.get().close();
      return;
    }
    mOutStream.ifPresent(FuseFileOutStream::close);
  }

  /**
   * @return true if the current stream is a write stream
   */
  public synchronized boolean isWriteStream() {
    return mOutStream.isPresent();
  }
}
