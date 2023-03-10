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

package alluxio.master.scheduler;

import alluxio.client.block.stream.BlockWorkerClient;
import alluxio.client.file.FileSystemContext;
import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.exception.runtime.UnavailableRuntimeException;
import alluxio.exception.status.UnavailableException;
import alluxio.master.file.FileSystemMaster;
import alluxio.resource.CloseableResource;
import alluxio.scheduler.job.WorkerProvider;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import java.io.IOException;
import java.util.List;

/**
 * Default worker provider that get worker information from Alluxio master.
 */
public class DefaultWorkerProvider implements WorkerProvider {
  private final FileSystemMaster mFileSystemMaster;
  private final FileSystemContext mContext;

  /**
   * Creates a new instance of {@link DefaultWorkerProvider}.
   *
   * @param fileSystemMaster the file system master
   * @param context the file system context
   */
  public DefaultWorkerProvider(FileSystemMaster fileSystemMaster, FileSystemContext context) {
    mFileSystemMaster = fileSystemMaster;
    mContext = context;
  }

  @Override
  public List<WorkerInfo> getWorkerInfos() {
    try {
      // TODO(jianjian): need api for healthy worker instead
      return  mFileSystemMaster.getWorkerInfoList();
    } catch (UnavailableException e) {
      throw new UnavailableRuntimeException(
          "fail to get worker infos because master is not available", e);
    }
  }

  @Override
  public CloseableResource<BlockWorkerClient> getWorkerClient(WorkerNetAddress address) {
    try {
      return mContext.acquireBlockWorkerClient(address);
    } catch (IOException e) {
      throw AlluxioRuntimeException.from(e);
    }
  }
}
