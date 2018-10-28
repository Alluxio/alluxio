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
import alluxio.exception.status.UnavailableException;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.FileSystemMasterJobService;
import alluxio.thrift.GetFileInfoTOptions;
import alluxio.thrift.GetFileInfoTResponse;
import alluxio.thrift.GetServiceVersionTOptions;
import alluxio.thrift.GetServiceVersionTResponse;
import alluxio.thrift.GetUfsInfoTOptions;
import alluxio.thrift.GetUfsInfoTResponse;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class is a Thrift handler for file system master RPCs invoked by Alluxio job service.
 */
@NotThreadSafe // TODO(jiri): make thread-safe (c.f. ALLUXIO-1664)
public final class FileSystemMasterJobServiceHandler implements FileSystemMasterJobService.Iface {
  private static final Logger LOG = LoggerFactory
      .getLogger(FileSystemMasterJobServiceHandler.class);

  private final FileSystemMaster mFileSystemMaster;

  /**
   * Creates a new instance of {@link FileSystemMasterJobServiceHandler}.
   *
   * @param fileSystemMaster the {@link FileSystemMaster} the handler uses internally
   */
  FileSystemMasterJobServiceHandler(FileSystemMaster fileSystemMaster) {
    Preconditions.checkNotNull(fileSystemMaster);
    mFileSystemMaster = fileSystemMaster;
  }

  @Override
  public GetServiceVersionTResponse getServiceVersion(GetServiceVersionTOptions options) {
    return new GetServiceVersionTResponse(Constants.FILE_SYSTEM_MASTER_WORKER_SERVICE_VERSION);
  }

  @Override
  public GetFileInfoTResponse getFileInfo(final long fileId, GetFileInfoTOptions options)
      throws AlluxioTException {
    return RpcUtils.call(LOG, (RpcUtils.RpcCallableThrowsIOException<GetFileInfoTResponse>) () -> {
      // TODO(ggezer)
      //try {
        // return new GetFileInfoTResponse(mFileSystemMaster.getFileInfo(fileId).toThrift());
        return null;
      //} catch (UnavailableException e) {
      //  throw new AlluxioException("File system master is unavailable", e);
      //}
    }, "GetFileInfo", "fileId=%s, options=%s", fileId, options);
  }

  @Override
  public GetUfsInfoTResponse getUfsInfo(final long mountId, GetUfsInfoTOptions options)
      throws AlluxioTException {
    // TODO(ggezer)
    return RpcUtils.call(
        LOG,
        (RpcUtils.RpcCallable<GetUfsInfoTResponse>) () -> null/*new GetUfsInfoTResponse(mFileSystemMaster
            .getUfsInfo(mountId))*/, "GetUfsInfo", "mountId=%s, options=%s", mountId, options);
  }
}
