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

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.RpcUtils;
import alluxio.exception.AlluxioException;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.CancelUfsFileTOptions;
import alluxio.thrift.CloseUfsFileTOptions;
import alluxio.thrift.CompleteUfsFileTOptions;
import alluxio.thrift.CreateUfsFileTOptions;
import alluxio.thrift.FileSystemWorkerClientService;
import alluxio.thrift.OpenUfsFileTOptions;
import alluxio.thrift.ThriftIOException;
import alluxio.worker.file.options.CompleteUfsFileOptions;
import alluxio.worker.file.options.CreateUfsFileOptions;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Handles incoming thrift requests from a worker file system client. This is mostly a wrapper
 * around {@link FileSystemWorker} and delegates calls to it.
 */
@NotThreadSafe
public final class FileSystemWorkerClientServiceHandler
    implements FileSystemWorkerClientService.Iface {
  private static final Logger LOG =
      LoggerFactory.getLogger(FileSystemWorkerClientServiceHandler.class);

  /** File System Worker that carries out most of the operations. */
  private final FileSystemWorker mWorker;

  /**
   * Creates a new instance of this class. This should only be called from {@link FileSystemWorker}
   *
   * @param worker the file system worker which will handle most of the requests
   */
  public FileSystemWorkerClientServiceHandler(FileSystemWorker worker) {
    mWorker = Preconditions.checkNotNull(worker, "worker");
  }

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
   * @throws AlluxioTException if an internal Alluxio error occurs
   * @throws ThriftIOException if an error occurs outside of Alluxio
   */
  @Override
  public void cancelUfsFile(final long sessionId, final long tempUfsFileId,
      final CancelUfsFileTOptions options) throws AlluxioTException, ThriftIOException {
    RpcUtils.call(LOG, new RpcUtils.RpcCallableThrowsIOException<Void>() {
      @Override
      public Void call() throws AlluxioException, IOException {
        mWorker.cancelUfsFile(sessionId, tempUfsFileId);
        return null;
      }

      @Override
      public String toString() {
        return String.format("CancelUfsFile. sessionId:%s, tempUfsFileId:%s, options:%s",
            sessionId, tempUfsFileId, options);
      }
    });
  }

  /**
   * Closes a file in the under file system which was opened for reading. The ufs id will be
   * invalid after this call.
   *
   * @param tempUfsFileId the worker specific file id of the ufs file
   * @param options the options for closing the file
   * @throws AlluxioTException if an internal Alluxio error occurs
   * @throws ThriftIOException if an error occurs outside of Alluxio
   */
  @Override
  public void closeUfsFile(final long sessionId, final long tempUfsFileId,
      final CloseUfsFileTOptions options) throws AlluxioTException, ThriftIOException {
    RpcUtils.call(LOG, new RpcUtils.RpcCallableThrowsIOException<Void>() {
      @Override
      public Void call() throws AlluxioException, IOException {
        mWorker.closeUfsFile(sessionId, tempUfsFileId);
        return null;
      }

      @Override
      public String toString() {
        return String.format("CloseUfsFile. sessionId:%s, tempUfsFileId:%s, options:%s",
            sessionId, tempUfsFileId, options);
      }
    });
  }

  /**
   * Completes the write to the file in the under file system specified by the worker file id. The
   * temporary file will be automatically promoted to the final file if possible.
   *
   * @param tempUfsFileId the worker id of the ufs file
   * @param options the options for completing the file
   * @return the length of the completed file
   * @throws AlluxioTException if an internal Alluxio error occurs
   * @throws ThriftIOException if an error occurs outside of Alluxio
   */
  @Override
  public long completeUfsFile(final long sessionId, final long tempUfsFileId,
      final CompleteUfsFileTOptions options) throws AlluxioTException, ThriftIOException {
    long ret = RpcUtils.call(LOG, new RpcUtils.RpcCallableThrowsIOException<Long>() {
      @Override
      public Long call() throws AlluxioException, IOException {
        return mWorker
            .completeUfsFile(sessionId, tempUfsFileId, new CompleteUfsFileOptions(options));
      }

      @Override
      public String toString() {
        return String.format("CompleteUfsFile. sessionId:%s, tempUfsFileId:%s, options:%s",
            sessionId, tempUfsFileId, options);
      }
    });
    return ret;
  }

  /**
   * Creates a file in the under file system. The file will be a temporary file until {@link
   * #completeUfsFile} is called.
   *
   * @param ufsUri the path of the file in the ufs
   * @param options the options for creating the file
   * @return the temporary worker specific file id which references the in-progress ufs file, all
   *         future operations on the file use this id until it is canceled or completed
   * @throws AlluxioTException if an internal Alluxio error occurs
   * @throws ThriftIOException if an error occurs outside of Alluxio
   */
  @Override
  public long createUfsFile(final long sessionId, final String ufsUri,
      final CreateUfsFileTOptions options) throws AlluxioTException, ThriftIOException {
    return RpcUtils.call(LOG, new RpcUtils.RpcCallableThrowsIOException<Long>() {
      @Override
      public Long call() throws AlluxioException, IOException {
        return mWorker
            .createUfsFile(sessionId, new AlluxioURI(ufsUri), new CreateUfsFileOptions(options));
      }

      @Override
      public String toString() {
        return String.format("CreateUfsFile. sessionId:%s, ufsUri:%s, options:%s", sessionId,
            ufsUri, options);
      }
    });
  }

  /**
   * Opens a file in the under file system.
   *
   * @param ufsUri the path of the file in the ufs
   * @param options the options for opening the file
   * @return the temporary worker specific file id which references the opened ufs file, all
   *         future operations from the reader should use this id until the file is closed
   * @throws AlluxioTException if an internal Alluxio error occurs
   * @throws ThriftIOException if an error occurs outside of Alluxio
   */
  @Override
  public long openUfsFile(final long sessionId, final String ufsUri,
      final OpenUfsFileTOptions options) throws AlluxioTException, ThriftIOException {
    return RpcUtils.call(LOG, new RpcUtils.RpcCallableThrowsIOException<Long>() {
      @Override
      public Long call() throws AlluxioException, IOException {
        return mWorker.openUfsFile(sessionId, new AlluxioURI(ufsUri));
      }

      @Override
      public String toString() {
        return String.format("OpenUfsFile. sessionId:%s, ufsUri:%s, options:%s", sessionId, ufsUri,
            options);
      }
    });
  }

  /**
   * Session heartbeat to worker to keep its state, for example open ufs streams.
   *
   * @param sessionId the session id of the client sending the heartbeat
   * @param metrics a list of the client metrics that were collected since the last heartbeat
   * @throws AlluxioTException if an internal Alluxio error occurs
   */
  @Override
  public void sessionHeartbeat(final long sessionId, final List<Long> metrics)
      throws AlluxioTException {
    RpcUtils.call(new RpcUtils.RpcCallable<Void>() {
      @Override
      public Void call() throws AlluxioException {
        mWorker.sessionHeartbeat(sessionId);
        return null;
      }
    });
  }
}
