/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.file;

import alluxio.Configuration;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.IdUtils;
import alluxio.util.io.PathUtils;
import alluxio.worker.WorkerContext;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Handles writes to the under file system. Manages open output streams to Under File Systems.
 * Individual streams should only be used by one client. Each instance keeps its own state of
 * temporary file ids to open streams.
 */
@ThreadSafe
public final class UnderFileSystemManager {
  /**
   * A wrapper around the output stream to the under file system. This class handles writing the
   * data to a temporary file. When the stream is closed, the temporary file will attempt to be
   * renamed to the final file path. This stream guarantees the temporary file will be cleaned up
   * when close or cancel is called.
   */
  // TODO(calvin): This can be defined by the UnderFileSystem
  private final class UnderFileSystemOutputStream {
    /** Configuration to use for this stream. */
    private final Configuration mConf;
    /** Underlying stream to the under file system file. */
    private final OutputStream mStream;
    /** Final path to write to in the under file system. */
    private final String mPath;
    /** Temporary path to write to in the under file system. */
    private final String mTemporaryPath;

    private UnderFileSystemOutputStream(String ufsPath, Configuration conf)
        throws FileAlreadyExistsException, IOException {
      mConf = Preconditions.checkNotNull(conf);
      mPath = Preconditions.checkNotNull(ufsPath);
      mTemporaryPath = PathUtils.temporaryFileName(IdUtils.getRandomNonNegativeLong(), mPath);
      UnderFileSystem ufs = UnderFileSystem.get(mPath, mConf);
      if (ufs.exists(mPath)) {
        throw new FileAlreadyExistsException(ExceptionMessage.FAILED_UFS_CREATE.getMessage(mPath));
      }
      mStream = ufs.create(mTemporaryPath);
    }

    /**
     * Closes the temporary file and deletes it. This object should be discarded after this call.
     *
     * @throws IOException if an error occurs during the under file system operation
     */
    private void cancel() throws IOException {
      mStream.close();
      UnderFileSystem ufs = UnderFileSystem.get(mPath, mConf);
      // TODO(calvin): Log a warning if the delete fails
      ufs.delete(mTemporaryPath, false);
    }

    /**
     * Closes the temporary file and attempts to promote it to the final file path. If the final
     * path already exists, the stream is canceled instead.
     *
     * @throws IOException if an error occurs during the under file system operation
     */
    private void complete() throws IOException {
      mStream.close();
      UnderFileSystem ufs = UnderFileSystem.get(mPath, mConf);
      if (!ufs.rename(mTemporaryPath, mPath)) {
        // TODO(calvin): Log a warning if the delete fails
        ufs.delete(mTemporaryPath, false);
      }
    }

    /**
     * @return the wrapped output stream to the under file system file
     */
    private OutputStream getStream() {
      return mStream;
    }
  }

  /** A random id generator for worker file ids. */
  private AtomicLong mIdGenerator;
  /** Map of worker file ids to open under file system streams. */
  private ConcurrentMap<Long, UnderFileSystemOutputStream> mStreams;

  /**
   * Creates a new under file system manager. Stream ids are unique to each under file system
   * manager.
   */
  public UnderFileSystemManager() {
    mIdGenerator = new AtomicLong(IdUtils.getRandomNonNegativeLong());
    mStreams = new ConcurrentHashMap<>();
  }

  /**
   * Creates a {@link UnderFileSystemOutputStream} for the given file path and adds it to the
   * open streams map keyed with the returned worker file id.
   *
   * @param ufsPath the path to create in the under file system
   * @return the worker file id which should be used to reference the open stream
   * @throws FileAlreadyExistsException if the under file system path already exists
   * @throws IOException if an error occurs when operating on the under file system
   */
  public long createFile(String ufsPath) throws FileAlreadyExistsException, IOException {
    UnderFileSystemOutputStream stream =
        new UnderFileSystemOutputStream(ufsPath, WorkerContext.getConf());
    long workerFileId = mIdGenerator.getAndIncrement();
    mStreams.put(workerFileId, stream);
    return workerFileId;
  }

  /**
   * Cancels the open stream associated with the given worker file id. This will close the open
   * stream.
   *
   * @param workerFileId the worker id referencing an open file in the under file system
   * @throws FileDoesNotExistException if the worker file id is not valid
   * @throws IOException if an error occurs when operating on the under file system
   */
  public void cancelFile(long workerFileId) throws FileDoesNotExistException, IOException {
    UnderFileSystemOutputStream stream = mStreams.remove(workerFileId);
    if (stream != null) {
      stream.cancel();
    } else {
      throw new FileDoesNotExistException(
          ExceptionMessage.BAD_WORKER_FILE_ID.getMessage(workerFileId));
    }
  }

  /**
   * Completes an open stream associated with the given worker file id. This will close the open
   * stream.
   *
   * @param workerFileId the worker id referencing an open file in the under file system
   * @throws FileDoesNotExistException if the worker file id is not valid
   * @throws IOException if an error occurs when operating on the under file system
   */
  public void completeFile(long workerFileId) throws FileDoesNotExistException, IOException {
    UnderFileSystemOutputStream stream = mStreams.remove(workerFileId);
    if (stream != null) {
      stream.complete();
    } else {
      throw new FileDoesNotExistException(
          ExceptionMessage.BAD_WORKER_FILE_ID.getMessage(workerFileId));
    }
  }
}
