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

package alluxio.dora.worker.file;

import alluxio.dora.AbstractMasterClient;
import alluxio.dora.Constants;
import alluxio.dora.conf.PropertyKey;
import alluxio.dora.grpc.FileSystemMasterWorkerServiceGrpc;
import alluxio.dora.grpc.GetFileInfoPRequest;
import alluxio.dora.grpc.GetPinnedFileIdsPRequest;
import alluxio.dora.grpc.GetUfsInfoPRequest;
import alluxio.dora.grpc.GrpcUtils;
import alluxio.dora.grpc.ServiceType;
import alluxio.dora.grpc.UfsInfo;
import alluxio.dora.master.MasterClientContext;
import alluxio.dora.wire.FileInfo;

import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A wrapper for the gRPC client to interact with the file system master, used by Alluxio worker.
 */
@ThreadSafe
public class FileSystemMasterClient extends AbstractMasterClient {
  private static final Logger LOG = LoggerFactory.getLogger(FileSystemMasterClient.class);
  private FileSystemMasterWorkerServiceGrpc.FileSystemMasterWorkerServiceBlockingStub mClient =
      null;

  /**
   * Creates a instance of {@link FileSystemMasterClient}.
   *
   * @param conf master client configuration
   */
  @Inject
  public FileSystemMasterClient(MasterClientContext conf) {
    super(conf);
  }

  @Override
  protected ServiceType getRemoteServiceType() {
    return ServiceType.FILE_SYSTEM_MASTER_WORKER_SERVICE;
  }

  @Override
  protected String getServiceName() {
    return Constants.FILE_SYSTEM_MASTER_WORKER_SERVICE_NAME;
  }

  @Override
  protected long getServiceVersion() {
    return Constants.FILE_SYSTEM_MASTER_WORKER_SERVICE_VERSION;
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
   * @return the set of pinned file ids
   */
  public Set<Long> getPinList() throws IOException {
    return retryRPC(() -> new HashSet<>(mClient.withDeadlineAfter(mContext.getClusterConf()
            .getMs(PropertyKey.WORKER_MASTER_PERIODICAL_RPC_TIMEOUT), TimeUnit.MILLISECONDS)
            .getPinnedFileIds(GetPinnedFileIdsPRequest.newBuilder().build())
            .getPinnedFileIdsList()),
        LOG, "GetPinList", "");
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
