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
import alluxio.thrift.FileSystemWorkerClientService;
import alluxio.thrift.ThriftIOException;
import alluxio.thrift.UFSCancelFileTOptions;
import alluxio.thrift.UFSCompleteFileTOptions;
import alluxio.thrift.UFSCreateFileTOptions;

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
   * @param workerFileId the worker id of the ufs file
   * @param options the options for canceling the file
   * @throws AlluxioTException if an internal Alluxio error occurs
   * @throws ThriftIOException if an error occurs outside of Alluxio
   */
  @Override
  public void ufsCancelFile(long workerFileId, UFSCancelFileTOptions options)
      throws AlluxioTException, ThriftIOException {
    try {
      mWorker.ufsCancelFile(workerFileId);
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
   * @param workerFileId the worker id of the ufs file
   * @param options the options for completing the file
   * @throws AlluxioTException if an internal Alluxio error occurs
   * @throws ThriftIOException if an error occurs outside of Alluxio
   */
  @Override
  public void ufsCompleteFile(long workerFileId, UFSCompleteFileTOptions options)
      throws AlluxioTException, ThriftIOException {
    try {
      mWorker.ufsCompleteFile(workerFileId);
    } catch (IOException e) {
      throw new ThriftIOException(e.getMessage());
    } catch (AlluxioException e) {
      throw e.toAlluxioTException();
    }
  }

  /**
   * Creates a file in the under file system. The file will be a temporary file until {@link
   * #ufsCompleteFile} is called.
   *
   * @param ufsPath the path of the file in the ufs
   * @param options the options for creating the file
   * @return the worker file id assigned to this file, all future access to the file uses this id
   * @throws AlluxioTException if an internal Alluxio error occurs
   * @throws ThriftIOException if an error occurs outside of Alluxio
   */
  @Override
  public long ufsCreateFile(String ufsPath, UFSCreateFileTOptions options)
      throws AlluxioTException, ThriftIOException {
    try {
      return mWorker.ufsCreateFile(ufsPath);
    } catch (IOException e) {
      throw new ThriftIOException(e.getMessage());
    } catch (AlluxioException e) {
      throw e.toAlluxioTException();
    }
  }
}
