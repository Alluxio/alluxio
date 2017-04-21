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

package alluxio.worker.file;

import alluxio.Constants;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.CancelUfsFileTOptions;
import alluxio.thrift.CloseUfsFileTOptions;
import alluxio.thrift.CompleteUfsFileTOptions;
import alluxio.thrift.CreateUfsFileTOptions;
import alluxio.thrift.FileSystemWorkerClientService;
import alluxio.thrift.OpenUfsFileTOptions;
<<<<<<< HEAD
import alluxio.thrift.ThriftIOException;
||||||| merged common ancestors
import alluxio.thrift.ThriftIOException;
import alluxio.worker.file.options.CompleteUfsFileOptions;
import alluxio.worker.file.options.CreateUfsFileOptions;
=======
import alluxio.worker.file.options.CompleteUfsFileOptions;
import alluxio.worker.file.options.CreateUfsFileOptions;
>>>>>>> upstream/exceptions

import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Handles incoming thrift requests from a worker file system client. These RPCs are no longer
 * supported as of 1.5.0.  All methods will throw {@link UnsupportedOperationException}.
 */
@NotThreadSafe
public final class FileSystemWorkerClientServiceHandler
    implements FileSystemWorkerClientService.Iface {
  private static final String UNSUPPORTED_MESSAGE = "Unsupported as of v1.5.0";

  /**
   * Creates a new instance of this class.
   */
  public FileSystemWorkerClientServiceHandler() {}

  @Override
  public long getServiceVersion() {
    return Constants.FILE_SYSTEM_WORKER_CLIENT_SERVICE_VERSION;
  }

  /**
   * Cancels the write to the file in the under file system specified by the worker file id. The
   * temporary file will be automatically cleaned up.
   *
   * @param tempUfsFileId the worker id of the ufs file
   * @param options the options for canceling the file
<<<<<<< HEAD
   * @throws UnsupportedOperationException always
||||||| merged common ancestors
   * @throws AlluxioTException if an internal Alluxio error occurs
   * @throws ThriftIOException if an error occurs outside of Alluxio
=======
   * @throws AlluxioTException if an error occurs
>>>>>>> upstream/exceptions
   */
  @Override
  public void cancelUfsFile(final long sessionId, final long tempUfsFileId,
<<<<<<< HEAD
      final CancelUfsFileTOptions options) throws AlluxioTException, ThriftIOException {
    throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
||||||| merged common ancestors
      final CancelUfsFileTOptions options) throws AlluxioTException, ThriftIOException {
    RpcUtils.callAndLog(LOG, new RpcUtils.RpcCallableThrowsIOException<Void>() {
      @Override
      public Void call() throws AlluxioException, IOException {
        mWorker.cancelUfsFile(sessionId, tempUfsFileId);
        return null;
      }

      @Override
      public String toString() {
        return String.format("CancelUfsFile: sessionId=%s, tempUfsFileId=%s, options=%s",
            sessionId, tempUfsFileId, options);
      }
    });
=======
      final CancelUfsFileTOptions options) throws AlluxioTException {
    RpcUtils.callAndLog(LOG, new RpcUtils.RpcCallableThrowsIOException<Void>() {
      @Override
      public Void call() throws AlluxioException, IOException {
        mWorker.cancelUfsFile(sessionId, tempUfsFileId);
        return null;
      }

      @Override
      public String toString() {
        return String.format("CancelUfsFile: sessionId=%s, tempUfsFileId=%s, options=%s",
            sessionId, tempUfsFileId, options);
      }
    });
>>>>>>> upstream/exceptions
  }

  /**
   * Closes a file in the under file system which was opened for reading. The ufs id will be
   * invalid after this call.
   *
   * @param tempUfsFileId the worker specific file id of the ufs file
   * @param options the options for closing the file
<<<<<<< HEAD
   * @throws UnsupportedOperationException always
||||||| merged common ancestors
   * @throws AlluxioTException if an internal Alluxio error occurs
   * @throws ThriftIOException if an error occurs outside of Alluxio
=======
   * @throws AlluxioTException if an error occurs
>>>>>>> upstream/exceptions
   */
  @Override
  public void closeUfsFile(final long sessionId, final long tempUfsFileId,
<<<<<<< HEAD
      final CloseUfsFileTOptions options) throws AlluxioTException, ThriftIOException {
    throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
||||||| merged common ancestors
      final CloseUfsFileTOptions options) throws AlluxioTException, ThriftIOException {
    RpcUtils.callAndLog(LOG, new RpcUtils.RpcCallableThrowsIOException<Void>() {
      @Override
      public Void call() throws AlluxioException, IOException {
        mWorker.closeUfsFile(sessionId, tempUfsFileId);
        return null;
      }

      @Override
      public String toString() {
        return String.format("CloseUfsFile: sessionId=%s, tempUfsFileId=%s, options=%s",
            sessionId, tempUfsFileId, options);
      }
    });
=======
      final CloseUfsFileTOptions options) throws AlluxioTException {
    RpcUtils.callAndLog(LOG, new RpcUtils.RpcCallableThrowsIOException<Void>() {
      @Override
      public Void call() throws AlluxioException, IOException {
        mWorker.closeUfsFile(sessionId, tempUfsFileId);
        return null;
      }

      @Override
      public String toString() {
        return String.format("CloseUfsFile: sessionId=%s, tempUfsFileId=%s, options=%s",
            sessionId, tempUfsFileId, options);
      }
    });
>>>>>>> upstream/exceptions
  }

  /**
   * Completes the write to the file in the under file system specified by the worker file id. The
   * temporary file will be automatically promoted to the final file if possible.
   *
   * @param tempUfsFileId the worker id of the ufs file
   * @param options the options for completing the file
<<<<<<< HEAD
   * @return this method always throws an exception
   * @throws UnsupportedOperationException always
||||||| merged common ancestors
   * @return the length of the completed file
   * @throws AlluxioTException if an internal Alluxio error occurs
   * @throws ThriftIOException if an error occurs outside of Alluxio
=======
   * @return the length of the completed file
   * @throws AlluxioTException if an error occurs
>>>>>>> upstream/exceptions
   */
  @Override
  public long completeUfsFile(final long sessionId, final long tempUfsFileId,
<<<<<<< HEAD
      final CompleteUfsFileTOptions options) throws AlluxioTException, ThriftIOException {
    throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
||||||| merged common ancestors
      final CompleteUfsFileTOptions options) throws AlluxioTException, ThriftIOException {
    return RpcUtils.callAndLog(LOG, new RpcUtils.RpcCallableThrowsIOException<Long>() {
      @Override
      public Long call() throws AlluxioException, IOException {
        return mWorker
            .completeUfsFile(sessionId, tempUfsFileId, new CompleteUfsFileOptions(options));
      }

      @Override
      public String toString() {
        return String.format("CompleteUfsFile: sessionId=%s, tempUfsFileId=%s, options=%s",
            sessionId, tempUfsFileId, options);
      }
    });
=======
      final CompleteUfsFileTOptions options) throws AlluxioTException {
    return RpcUtils.callAndLog(LOG, new RpcUtils.RpcCallableThrowsIOException<Long>() {
      @Override
      public Long call() throws AlluxioException, IOException {
        return mWorker
            .completeUfsFile(sessionId, tempUfsFileId, new CompleteUfsFileOptions(options));
      }

      @Override
      public String toString() {
        return String.format("CompleteUfsFile: sessionId=%s, tempUfsFileId=%s, options=%s",
            sessionId, tempUfsFileId, options);
      }
    });
>>>>>>> upstream/exceptions
  }

  /**
   * Creates a file in the under file system. The file will be a temporary file until {@link
   * #completeUfsFile} is called.
   *
   * @param ufsUri the path of the file in the ufs
   * @param options the options for creating the file
<<<<<<< HEAD
   * @return this method always throws an exception
   * @throws UnsupportedOperationException always
||||||| merged common ancestors
   * @return the temporary worker specific file id which references the in-progress ufs file, all
   *         future operations on the file use this id until it is canceled or completed
   * @throws AlluxioTException if an internal Alluxio error occurs
   * @throws ThriftIOException if an error occurs outside of Alluxio
=======
   * @return the temporary worker specific file id which references the in-progress ufs file, all
   *         future operations on the file use this id until it is canceled or completed
   * @throws AlluxioTException if an error occurs
>>>>>>> upstream/exceptions
   */
  @Override
  public long createUfsFile(final long sessionId, final String ufsUri,
<<<<<<< HEAD
      final CreateUfsFileTOptions options) throws AlluxioTException, ThriftIOException {
    throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
||||||| merged common ancestors
      final CreateUfsFileTOptions options) throws AlluxioTException, ThriftIOException {
    return RpcUtils.callAndLog(LOG, new RpcUtils.RpcCallableThrowsIOException<Long>() {
      @Override
      public Long call() throws AlluxioException, IOException {
        return mWorker
            .createUfsFile(sessionId, new AlluxioURI(ufsUri), new CreateUfsFileOptions(options));
      }

      @Override
      public String toString() {
        return String.format("CreateUfsFile: sessionId=%s, ufsUri=%s, options=%s", sessionId,
            ufsUri, options);
      }
    });
=======
      final CreateUfsFileTOptions options) throws AlluxioTException {
    return RpcUtils.callAndLog(LOG, new RpcUtils.RpcCallableThrowsIOException<Long>() {
      @Override
      public Long call() throws AlluxioException, IOException {
        return mWorker
            .createUfsFile(sessionId, new AlluxioURI(ufsUri), new CreateUfsFileOptions(options));
      }

      @Override
      public String toString() {
        return String.format("CreateUfsFile: sessionId=%s, ufsUri=%s, options=%s", sessionId,
            ufsUri, options);
      }
    });
>>>>>>> upstream/exceptions
  }

  /**
   * Opens a file in the under file system.
   *
   * @param ufsUri the path of the file in the ufs
   * @param options the options for opening the file
<<<<<<< HEAD
   * @return this method always throws an exception
   * @throws UnsupportedOperationException always
||||||| merged common ancestors
   * @return the temporary worker specific file id which references the opened ufs file, all
   *         future operations from the reader should use this id until the file is closed
   * @throws AlluxioTException if an internal Alluxio error occurs
   * @throws ThriftIOException if an error occurs outside of Alluxio
=======
   * @return the temporary worker specific file id which references the opened ufs file, all
   *         future operations from the reader should use this id until the file is closed
   * @throws AlluxioTException if an error occurs
>>>>>>> upstream/exceptions
   */
  @Override
  public long openUfsFile(final long sessionId, final String ufsUri,
<<<<<<< HEAD
      final OpenUfsFileTOptions options) throws AlluxioTException, ThriftIOException {
    throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
||||||| merged common ancestors
      final OpenUfsFileTOptions options) throws AlluxioTException, ThriftIOException {
    return RpcUtils.callAndLog(LOG, new RpcUtils.RpcCallableThrowsIOException<Long>() {
      @Override
      public Long call() throws AlluxioException, IOException {
        return mWorker.openUfsFile(sessionId, new AlluxioURI(ufsUri));
      }

      @Override
      public String toString() {
        return String.format("OpenUfsFile: sessionId=%s, ufsUri=%s, options=%s", sessionId, ufsUri,
            options);
      }
    });
=======
      final OpenUfsFileTOptions options) throws AlluxioTException {
    return RpcUtils.callAndLog(LOG, new RpcUtils.RpcCallableThrowsIOException<Long>() {
      @Override
      public Long call() throws AlluxioException, IOException {
        return mWorker.openUfsFile(sessionId, new AlluxioURI(ufsUri));
      }

      @Override
      public String toString() {
        return String.format("OpenUfsFile: sessionId=%s, ufsUri=%s, options=%s", sessionId, ufsUri,
            options);
      }
    });
>>>>>>> upstream/exceptions
  }

  /**
   * Session heartbeat to worker to keep its state, for example open ufs streams.
   *
   * @param sessionId the session id of the client sending the heartbeat
   * @param metrics a list of the client metrics that were collected since the last heartbeat
<<<<<<< HEAD
   * @throws UnsupportedOperationException always
||||||| merged common ancestors
   * @throws AlluxioTException if an internal Alluxio error occurs
=======
   * @throws AlluxioTException if an error occurs
>>>>>>> upstream/exceptions
   */
  @Override
  public void sessionHeartbeat(final long sessionId, final List<Long> metrics)
      throws AlluxioTException {
    throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
  }
}
