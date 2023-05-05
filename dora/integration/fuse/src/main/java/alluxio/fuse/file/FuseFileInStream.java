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
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.concurrent.LockMode;
import alluxio.exception.AlluxioException;
import alluxio.exception.PreconditionMessage;
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
 * An implementation for {@link FuseFileStream} for read only operations against an Alluxio uri.
 */
@ThreadSafe
public class FuseFileInStream implements FuseFileStream {
  private final FileInStream mInStream;
  private final FileStatus mFileStatus;
  private final AlluxioURI mURI;
  private final CloseableResource<Lock> mLockResource;
  private volatile boolean mClosed = false;

  /**
   * Creates a {@link FuseFileInStream}.
   *
   * @param fileSystem the file system
   * @param lockManager the lock manager
   * @param uri the alluxio uri
   * @return a {@link FuseFileInStream}
   */
  public static FuseFileInStream create(FileSystem fileSystem, FuseReadWriteLockManager lockManager,
      AlluxioURI uri) {
    Preconditions.checkNotNull(fileSystem);
    Preconditions.checkNotNull(uri);
    // Make sure file is not being written by current FUSE
    // deal with the async Fuse.release issue by waiting for write lock to be released
    CloseableResource<Lock> lockResource = lockManager.tryLock(uri.toString(), LockMode.READ);

    try {
      // Make sure file is not being written by other clients outside current FUSE
      Optional<URIStatus> status = AlluxioFuseUtils.getPathStatus(fileSystem, uri);
      if (status.isPresent() && !status.get().isCompleted()) {
        status = AlluxioFuseUtils.waitForFileCompleted(fileSystem, uri);
        if (!status.isPresent()) {
          throw new UnimplementedRuntimeException(String.format(
              "Failed to create fuse file in stream for %s: file is being written", uri));
        }
      }

      if (!status.isPresent()) {
        throw new NotFoundRuntimeException(String.format(
            "Failed to create read-only stream for %s: file does not exist", uri));
      }

      try {
        FileInStream is = fileSystem.openFile(status.get(),
            OpenFilePOptions.getDefaultInstance());
        return new FuseFileInStream(is, lockResource,
            new FileStatus(status.get().getLength()), uri);
      } catch (IOException | AlluxioException e) {
        throw new RuntimeException(e);
      }
    } catch (Throwable t) {
      lockResource.close();
      throw t;
    }
  }

  private FuseFileInStream(FileInStream inStream, CloseableResource<Lock> lockResource,
      FileStatus fileStatus, AlluxioURI uri) {
    mInStream = Preconditions.checkNotNull(inStream);
    mLockResource = Preconditions.checkNotNull(lockResource);
    mFileStatus = Preconditions.checkNotNull(fileStatus);
    mURI = Preconditions.checkNotNull(uri);
  }

  @Override
  public synchronized int read(ByteBuffer buf, long size, long offset) {
    Preconditions.checkArgument(size >= 0 && offset >= 0 && size <= buf.capacity(),
        PreconditionMessage.ERR_BUFFER_STATE.toString(), buf.capacity(), offset, size);
    if (size == 0) {
      return 0;
    }
    if (offset >= mFileStatus.getFileLength()) {
      return 0;
    }
    final int sz = (int) size;
    int totalRead = 0;
    int currentRead;
    try {
      mInStream.seek(offset);
      do {
        currentRead = mInStream.read(buf, totalRead, sz - totalRead);
        if (currentRead > 0) {
          totalRead += currentRead;
        }
      } while (currentRead > 0 && totalRead < sz);
    } catch (IOException e) {
      throw AlluxioRuntimeException.from(e);
    }
    return totalRead == 0 ? currentRead : totalRead;
  }

  @Override
  public void write(ByteBuffer buf, long size, long offset) {
    throw new FailedPreconditionRuntimeException(String
        .format("Cannot write to read-only stream of path %s", mURI));
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
        .format("Cannot truncate read-only stream of path %s", mURI));
  }

  @Override
  public synchronized void close() {
    if (mClosed) {
      return;
    }
    mClosed = true;
    try {
      mInStream.close();
    } catch (IOException e) {
      throw AlluxioRuntimeException.from(e);
    } finally {
      mLockResource.close();
    }
  }
}
