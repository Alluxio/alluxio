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
import alluxio.thrift.ThriftIOException;

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
   * @throws UnsupportedOperationException always
   */
  @Override
  public void cancelUfsFile(final long sessionId, final long tempUfsFileId,
      final CancelUfsFileTOptions options) throws AlluxioTException, ThriftIOException {
    throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
  }

  /**
   * Closes a file in the under file system which was opened for reading. The ufs id will be
   * invalid after this call.
   *
   * @param tempUfsFileId the worker specific file id of the ufs file
   * @param options the options for closing the file
   * @throws UnsupportedOperationException always
   */
  @Override
  public void closeUfsFile(final long sessionId, final long tempUfsFileId,
      final CloseUfsFileTOptions options) throws AlluxioTException, ThriftIOException {
    throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
  }

  /**
   * Completes the write to the file in the under file system specified by the worker file id. The
   * temporary file will be automatically promoted to the final file if possible.
   *
   * @param tempUfsFileId the worker id of the ufs file
   * @param options the options for completing the file
   * @return this method always throws an exception
   * @throws UnsupportedOperationException always
   */
  @Override
  public long completeUfsFile(final long sessionId, final long tempUfsFileId,
      final CompleteUfsFileTOptions options) throws AlluxioTException, ThriftIOException {
    throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
  }

  /**
   * Creates a file in the under file system. The file will be a temporary file until {@link
   * #completeUfsFile} is called.
   *
   * @param ufsUri the path of the file in the ufs
   * @param options the options for creating the file
   * @return this method always throws an exception
   * @throws UnsupportedOperationException always
   */
  @Override
  public long createUfsFile(final long sessionId, final String ufsUri,
      final CreateUfsFileTOptions options) throws AlluxioTException, ThriftIOException {
    throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
  }

  /**
   * Opens a file in the under file system.
   *
   * @param ufsUri the path of the file in the ufs
   * @param options the options for opening the file
   * @return this method always throws an exception
   * @throws UnsupportedOperationException always
   */
  @Override
  public long openUfsFile(final long sessionId, final String ufsUri,
      final OpenUfsFileTOptions options) throws AlluxioTException, ThriftIOException {
    throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
  }

  /**
   * Session heartbeat to worker to keep its state, for example open ufs streams.
   *
   * @param sessionId the session id of the client sending the heartbeat
   * @param metrics a list of the client metrics that were collected since the last heartbeat
   * @throws UnsupportedOperationException always
   */
  @Override
  public void sessionHeartbeat(final long sessionId, final List<Long> metrics)
      throws AlluxioTException {
    throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
  }
}
