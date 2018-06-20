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
import alluxio.RpcUtils.RpcCallable;
import alluxio.RpcUtils.RpcCallableThrowsIOException;
import alluxio.master.file.options.WorkerHeartbeatOptions;
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

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    return RpcUtils.call(LOG, (RpcCallableThrowsIOException<FileSystemHeartbeatTResponse>) () ->
        new FileSystemHeartbeatTResponse(mFileSystemMaster.workerHeartbeat(workerId, persistedFiles,
            new WorkerHeartbeatOptions(options))),
        "FileSystemHeartbeat", "workerId=%s, persistedFiles=%s, options=%s",
        workerId, persistedFiles, options);
  }

  @Override
  public GetFileInfoTResponse getFileInfo(final long fileId, GetFileInfoTOptions options)
      throws AlluxioTException {
    return RpcUtils.call(LOG, (RpcCallableThrowsIOException<GetFileInfoTResponse>) () ->
        new GetFileInfoTResponse(mFileSystemMaster.getFileInfo(fileId).toThrift()),
        "GetFileInfo", "fileId=%s, options=%s", fileId, options);
  }

  @Override
  public GetPinnedFileIdsTResponse getPinnedFileIds(GetPinnedFileIdsTOptions options)
      throws AlluxioTException {
    return RpcUtils.call(LOG, (RpcCallable<GetPinnedFileIdsTResponse>) () ->
        new GetPinnedFileIdsTResponse(mFileSystemMaster.getPinIdList()),
        "GetPinnedFileIds", "options=%s", options);
  }

  @Override
  public GetUfsInfoTResponse getUfsInfo(final long mountId, GetUfsInfoTOptions options)
      throws AlluxioTException {
    return RpcUtils.call(LOG, (RpcCallable<GetUfsInfoTResponse>) () ->
        new GetUfsInfoTResponse(mFileSystemMaster.getUfsInfo(mountId)),
        "GetUfsInfo", "mountId=%s, options=%s", mountId, options);
  }
}
