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

import alluxio.AbstractMasterClient;
import alluxio.Constants;
import alluxio.grpc.FileSystemMasterWorkerServiceGrpc;
import alluxio.grpc.GetFileInfoPRequest;
import alluxio.grpc.GetUfsInfoPRequest;
import alluxio.grpc.ServiceType;
import alluxio.grpc.UfsInfo;
import alluxio.master.MasterClientContext;
import alluxio.grpc.GrpcUtils;
import alluxio.wire.FileInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A wrapper for the gRPC client to interact with the file system master, used by Alluxio job
 * service.
 */
@ThreadSafe
public final class FileSystemMasterClient extends AbstractMasterClient {
  private static final Logger LOG = LoggerFactory.getLogger(FileSystemMasterClient.class);
  private FileSystemMasterWorkerServiceGrpc.FileSystemMasterWorkerServiceBlockingStub mClient =
          null;

  /**
   * Creates an instance of {@link FileSystemMasterClient}.
   *
   * @param conf master client configuration
   */
  public FileSystemMasterClient(MasterClientContext conf) {
    super(conf);
  }

  @Override
  protected ServiceType getRemoteServiceType() {
    return ServiceType.FILE_SYSTEM_MASTER_JOB_SERVICE;
  }

  @Override
  protected String getServiceName() {
    return Constants.FILE_SYSTEM_MASTER_JOB_SERVICE_NAME;
  }

  @Override
  protected long getServiceVersion() {
    return Constants.FILE_SYSTEM_MASTER_JOB_SERVICE_VERSION;
  }

  @Override
  protected void afterConnect() throws IOException {
    mClient = FileSystemMasterWorkerServiceGrpc.newBlockingStub(mChannel);
  }

  /**
   * @param fileId the id of the file for which to get the {@link FileInfo}
   * @return the file info for the given file id
   */
  public FileInfo getFileInfo(final long fileId) throws IOException {
    return retryRPC(() -> GrpcUtils.fromProto(mClient
        .getFileInfo(GetFileInfoPRequest.newBuilder().setFileId(fileId).build()).getFileInfo()),
        LOG, "GetFileInfo", "fileId=%d", fileId);
  }

  /**
   * @param mountId the id of the mount
   * @return the ufs information for the give ufs
   * @throws IOException if an I/O error occurs
   */
  public UfsInfo getUfsInfo(final long mountId) throws IOException {
    return retryRPC(() -> mClient
        .getUfsInfo(GetUfsInfoPRequest.newBuilder().setMountId(mountId).build()).getUfsInfo(),
        LOG, "GetUfsInfo", "mountId=%d", mountId);
  }
}
