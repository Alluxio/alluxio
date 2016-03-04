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
import alluxio.thrift.FileSystemWorkerClientService;
import alluxio.thrift.UFSCancelFileTOptions;
import alluxio.thrift.UFSCompleteFileTOptions;
import alluxio.thrift.UFSCreateFileTOptions;

/**
 * Handles incoming thrift requests from a worker file system client. This is mostly a wrapper
 * around {@link FileSystemWorker} and delegates calls to it.
 */
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
    mWorker = worker;
  }

  @Override
  public long getServiceVersion() {
    return Constants.FILE_SYSTEM_WORKER_CLIENT_SERVICE_VERSION;
  }

  @Override
  public void ufsCancelFile(String path, UFSCancelFileTOptions options) {

  }

  @Override
  public void ufsCompleteFile(String path, UFSCompleteFileTOptions options) {

  }

  @Override
  public void ufsCreateFile(String path, UFSCreateFileTOptions options) {

  }
}
