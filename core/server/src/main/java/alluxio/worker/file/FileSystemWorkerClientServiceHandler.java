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

import alluxio.Constants;
import alluxio.exception.AlluxioException;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.CancelUfsFileTOptions;
import alluxio.thrift.CompleteUfsFileTOptions;
import alluxio.thrift.CreateUfsFileTOptions;
import alluxio.thrift.FileSystemWorkerClientService;
import alluxio.thrift.ThriftIOException;

import com.google.common.base.Preconditions;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Handles incoming thrift requests from a worker file system client. This is mostly a wrapper
 * around {@link FileSystemWorker} and delegates calls to it.
 */
@NotThreadSafe
public final class FileSystemWorkerClientServiceHandler
    implements FileSystemWorkerClientService.Iface {

  /** File System Worker that carries out most of the operations. */
  private final FileSystemWorker mWorker;

  /**
   * Creates a new instance of this class. This should only be called from {@link FileSystemWorker}
   *
   * @param worker the file system worker which will handle most of the requests
   */
  public FileSystemWorkerClientServiceHandler(FileSystemWorker worker) {
    mWorker = Preconditions.checkNotNull(worker);
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
  public void cancelUfsFile(long tempUfsFileId, CancelUfsFileTOptions options)
      throws AlluxioTException, ThriftIOException {
    try {
      mWorker.cancelUfsFile(tempUfsFileId);
    } catch (IOException e) {
      throw new ThriftIOException(e.getMessage());
    } catch (AlluxioException e) {
      throw e.toAlluxioTException();
    }
  }

  /**
   * Completes the write to the file in the under file system specified by the worker file id. The
   * temporary file will be automatically promoted to the final file if possible.
   *
   * @param tempUfsFileId the worker id of the ufs file
   * @param options the options for completing the file
   * @throws AlluxioTException if an internal Alluxio error occurs
   * @throws ThriftIOException if an error occurs outside of Alluxio
   */
  @Override
  public void completeUfsFile(long tempUfsFileId, CompleteUfsFileTOptions options)
      throws AlluxioTException, ThriftIOException {
    try {
      mWorker.completeUfsFile(tempUfsFileId);
    } catch (IOException e) {
      throw new ThriftIOException(e.getMessage());
    } catch (AlluxioException e) {
      throw e.toAlluxioTException();
    }
  }

  /**
   * Creates a file in the under file system. The file will be a temporary file until {@link
   * #completeUfsFile} is called.
   *
   * @param ufsPath the path of the file in the ufs
   * @param options the options for creating the file
   * @return the temporary worker specific file id which references the in-progress ufs file, all
   *         future operations on the file use this id until it is canceled or completed
   * @throws AlluxioTException if an internal Alluxio error occurs
   * @throws ThriftIOException if an error occurs outside of Alluxio
   */
  @Override
  public long createUfsFile(String ufsPath, CreateUfsFileTOptions options)
      throws AlluxioTException, ThriftIOException {
    try {
      return mWorker.createUfsFile(ufsPath);
    } catch (IOException e) {
      throw new ThriftIOException(e.getMessage());
    } catch (AlluxioException e) {
      throw e.toAlluxioTException();
    }
  }
}
