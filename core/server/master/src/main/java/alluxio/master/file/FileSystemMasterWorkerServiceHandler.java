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

package alluxio.master.file;

import alluxio.Constants;
import alluxio.RpcUtils;
import alluxio.exception.AlluxioException;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.FileInfo;
import alluxio.thrift.FileSystemCommand;
import alluxio.thrift.FileSystemMasterWorkerService;
import alluxio.thrift.UfsInfo;
import alluxio.wire.ThriftUtils;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class is a Thrift handler for file system master RPCs invoked by an Alluxio worker.
 */
@NotThreadSafe // TODO(jiri): make thread-safe (c.f. ALLUXIO-1664)
public final class FileSystemMasterWorkerServiceHandler
    implements FileSystemMasterWorkerService.Iface {
  private static final Logger LOG =
      LoggerFactory.getLogger(FileSystemMasterWorkerServiceHandler.class);

  private final FileSystemMaster mFileSystemMaster;

  /**
   * Creates a new instance of {@link FileSystemMasterWorkerServiceHandler}.
   *
   * @param fileSystemMaster the {@link FileSystemMaster} the handler uses internally
   */
  public FileSystemMasterWorkerServiceHandler(FileSystemMaster fileSystemMaster) {
    Preconditions.checkNotNull(fileSystemMaster);
    mFileSystemMaster = fileSystemMaster;
  }

  @Override
  public long getServiceVersion() {
    return Constants.FILE_SYSTEM_MASTER_WORKER_SERVICE_VERSION;
  }

  @Override
  public FileInfo getFileInfo(final long fileId) throws AlluxioTException {
    return RpcUtils.call(LOG, new RpcUtils.RpcCallable<FileInfo>() {
      @Override
      public FileInfo call() throws AlluxioException {
        return ThriftUtils.toThrift(mFileSystemMaster.getFileInfo(fileId));
      }
    });
  }

  @Override
  public Set<Long> getPinIdList() throws AlluxioTException {
    return RpcUtils.call(LOG, new RpcUtils.RpcCallable<Set<Long>>() {
      @Override
      public Set<Long> call() throws AlluxioException {
        return mFileSystemMaster.getPinIdList();
      }
    });
  }

  @Override
  public UfsInfo getUfsInfo(final long mountId) throws AlluxioTException {
    return RpcUtils.call(LOG, new RpcUtils.RpcCallable<UfsInfo>() {
      @Override
      public UfsInfo call() throws AlluxioException {
        return mFileSystemMaster.getUfsInfo(mountId);
      }
    });
  }

  @Override
  public FileSystemCommand heartbeat(final long workerId, final List<Long> persistedFiles)
      throws AlluxioTException {
    return RpcUtils.call(LOG, new RpcUtils.RpcCallable<FileSystemCommand>() {
      @Override
      public FileSystemCommand call() throws AlluxioException {
        return mFileSystemMaster.workerHeartbeat(workerId, persistedFiles);
      }
    });
  }
}
