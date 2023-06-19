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
import alluxio.PositionReader;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.concurrent.LockMode;
import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.exception.runtime.FailedPreconditionRuntimeException;
import alluxio.exception.runtime.NotFoundRuntimeException;
import alluxio.exception.runtime.UnimplementedRuntimeException;
import alluxio.fuse.AlluxioFuseUtils;
import alluxio.fuse.lock.FuseReadWriteLockManager;
import alluxio.grpc.OpenFilePOptions;
import alluxio.resource.CloseableResource;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import javax.annotation.concurrent.ThreadSafe;

/**
 * FUSE position reader.
 */
@ThreadSafe
public class FusePositionReader implements FuseFileStream {
  /**
   * @param fileSystem
   * @param lockManager
   * @param uri
   * @return Fuse position reader
   */
  public static FusePositionReader create(
      FileSystem fileSystem, FuseReadWriteLockManager lockManager,
      AlluxioURI uri) {
    Preconditions.checkNotNull(fileSystem);
    Preconditions.checkNotNull(uri);
    // Make sure file is not being written by current FUSE
    // deal with the async Fuse.release issue by waiting for write lock to be released
    CloseableResource<Lock> lockResource = lockManager.tryLock(uri.toString(), LockMode.READ);

    try {
      // Make sure file is not being written by other clients outside current FUSE
      Optional<URIStatus> status = AlluxioFuseUtils.getPathStatus(fileSystem, uri);

      if (!status.isPresent()) {
        throw new NotFoundRuntimeException(String.format(
            "Failed to create read-only stream for %s: file does not exist", uri));
      }

      PositionReader reader = fileSystem.openPositionRead(status.get(),
          OpenFilePOptions.getDefaultInstance());
      return new FusePositionReader(reader, lockResource,
          new FileStatus(status.get().getLength()), uri);
    } catch (Throwable t) {
      lockResource.close();
      throw t;
    }
  }

  private final PositionReader mPositionReader;
  private final FileStatus mFileStatus;
  private final AlluxioURI mURI;
  private final CloseableResource<Lock> mLockResource;
  private volatile boolean mClosed = false;

  private FusePositionReader(PositionReader reader,
      CloseableResource<Lock> lockResource,
      FileStatus fileStatus, AlluxioURI uri) {
    mPositionReader = Preconditions.checkNotNull(reader);
    mLockResource = Preconditions.checkNotNull(lockResource);
    mFileStatus = Preconditions.checkNotNull(fileStatus);
    mURI = Preconditions.checkNotNull(uri);
  }

  @Override
  public int read(ByteBuffer buf, long size, long offset) {
    if (mClosed) {
      throw new FailedPreconditionRuntimeException("Position reader is closed");
    }
    if (offset >= mFileStatus.getFileLength()) {
      return 0;
    }
    try {
      return mPositionReader.read(offset, buf, (int) size);
    } catch (IOException e) {
      throw AlluxioRuntimeException.from(e);
    }
  }

  @Override
  public void write(ByteBuffer buf, long size, long offset) {
    throw new FailedPreconditionRuntimeException(String
        .format("Cannot write to reading file %s", mURI));
  }

  @Override
  public FileStatus getFileStatus() {
    return mFileStatus;
  }

  @Override
  public void flush() {}

  @Override
  public void truncate(long size) {
    throw new UnimplementedRuntimeException(String
        .format("Cannot truncate reading file  %s", mURI));
  }

  @Override
  public synchronized void close() {
    if (mClosed) {
      return;
    }
    mClosed = true;
    try {
      mPositionReader.close();
    } catch (IOException e) {
      throw AlluxioRuntimeException.from(e);
    } finally {
      mLockResource.close();
    }
  }
}
