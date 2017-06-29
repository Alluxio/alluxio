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
import alluxio.thrift.FileSystemHeartbeatTOptions;
import alluxio.thrift.FileSystemHeartbeatTResponse;
import alluxio.thrift.FileSystemMasterWorkerService;
import alluxio.thrift.GetFileInfoTOptions;
import alluxio.thrift.GetFileInfoTResponse;
import alluxio.thrift.GetPinnedFileIdsTOptions;
import alluxio.thrift.GetPinnedFileIdsTResponse;
import alluxio.thrift.GetServiceVersionTOptions;
import alluxio.thrift.GetServiceVersionTResponse;
import alluxio.thrift.GetUfsInfoTOptions;
import alluxio.thrift.GetUfsInfoTResponse;
import alluxio.wire.ThriftUtils;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

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
  FileSystemMasterWorkerServiceHandler(FileSystemMaster fileSystemMaster) {
    Preconditions.checkNotNull(fileSystemMaster);
    mFileSystemMaster = fileSystemMaster;
  }

  @Override
  public GetServiceVersionTResponse getServiceVersion(GetServiceVersionTOptions options) {
    return new GetServiceVersionTResponse(Constants.FILE_SYSTEM_MASTER_WORKER_SERVICE_VERSION);
  }

  @Override
  public FileSystemHeartbeatTResponse fileSystemHeartbeat(final long workerId,
      final List<Long> persistedFiles, FileSystemHeartbeatTOptions options)
      throws AlluxioTException {
    return RpcUtils.call(LOG,
        new RpcUtils.RpcCallableThrowsIOException<FileSystemHeartbeatTResponse>() {
          @Override
          public FileSystemHeartbeatTResponse call() throws AlluxioException, IOException {
            return new FileSystemHeartbeatTResponse(
                mFileSystemMaster.workerHeartbeat(workerId, persistedFiles));
          }
        }
    );
  }

  @Override
  public GetFileInfoTResponse getFileInfo(final long fileId, GetFileInfoTOptions options)
      throws AlluxioTException {
    return RpcUtils.call(LOG, new RpcUtils.RpcCallable<GetFileInfoTResponse>() {
      @Override
      public GetFileInfoTResponse call() throws AlluxioException {
        return new GetFileInfoTResponse(
            ThriftUtils.toThrift(mFileSystemMaster.getFileInfo(fileId)));
      }
    });
  }

  @Override
  public GetPinnedFileIdsTResponse getPinnedFileIds(GetPinnedFileIdsTOptions options)
      throws AlluxioTException {
    return RpcUtils.call(LOG, new RpcUtils.RpcCallable<GetPinnedFileIdsTResponse>() {
      @Override
      public GetPinnedFileIdsTResponse call() throws AlluxioException {
        return new GetPinnedFileIdsTResponse(mFileSystemMaster.getPinIdList());
      }
    });
  }

  @Override
  public GetUfsInfoTResponse getUfsInfo(final long mountId, GetUfsInfoTOptions options)
      throws AlluxioTException {
    return RpcUtils.call(LOG, new RpcUtils.RpcCallable<GetUfsInfoTResponse>() {
      @Override
      public GetUfsInfoTResponse call() throws AlluxioException {
        return new GetUfsInfoTResponse(mFileSystemMaster.getUfsInfo(mountId));
      }
    });
  }
}
