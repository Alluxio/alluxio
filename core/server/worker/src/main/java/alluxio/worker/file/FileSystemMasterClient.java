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

package alluxio.worker.file;

import alluxio.AbstractMasterClient;
import alluxio.Constants;
import alluxio.grpc.FileSystemCommand;
import alluxio.grpc.FileSystemHeartbeatPOptions;
import alluxio.grpc.FileSystemHeartbeatPRequest;
import alluxio.grpc.FileSystemMasterWorkerServiceGrpc;
import alluxio.grpc.GetFileInfoPRequest;
import alluxio.grpc.GetPinnedFileIdsPRequest;
import alluxio.grpc.GetUfsInfoPRequest;
import alluxio.grpc.ServiceType;
import alluxio.grpc.UfsInfo;
import alluxio.master.MasterClientConfig;
import alluxio.grpc.GrpcUtils;
import alluxio.wire.FileInfo;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A wrapper for the gRPC client to interact with the file system master, used by Alluxio worker.
 */
@ThreadSafe
public final class FileSystemMasterClient extends AbstractMasterClient {
  private FileSystemMasterWorkerServiceGrpc.FileSystemMasterWorkerServiceBlockingStub mClient =
      null;

  /**
   * Creates a instance of {@link FileSystemMasterClient}.
   *
   * @param conf master client configuration
   */
  public FileSystemMasterClient(MasterClientConfig conf) {
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
        .getFileInfo(GetFileInfoPRequest.newBuilder().setFileId(fileId).build()).getFileInfo()));
  }

  /**
   * @return the set of pinned file ids
   */
  public Set<Long> getPinList() throws IOException {
    return retryRPC(() -> new HashSet<>(mClient
        .getPinnedFileIds(GetPinnedFileIdsPRequest.newBuilder().build()).getPinnedFileIdsList()));
  }

  /**
   * @param mountId the id of the mount
   * @return the ufs information for the give ufs
   * @throws IOException if an I/O error occurs
   */
  public UfsInfo getUfsInfo(final long mountId) throws IOException {
    return retryRPC(() -> mClient
        .getUfsInfo(GetUfsInfoPRequest.newBuilder().setMountId(mountId).build()).getUfsInfo());
  }

  /**
   * Heartbeats to the master. It also carries command for the worker to persist the given files.
   *
   * @param workerId the id of the worker that heartbeats
   * @param persistedFiles the files which have been persisted since the last heartbeat
   * @return the command for file system worker
   */
  public FileSystemCommand heartbeat(final long workerId,
      final List<Long> persistedFiles) throws IOException {
    return heartbeat(workerId, persistedFiles, FileSystemHeartbeatPOptions.newBuilder().build());
  }

  /**
   * Heartbeats to the master. It also carries command for the worker to persist the given files.
   *
   * @param workerId the id of the worker that heartbeats
   * @param persistedFiles the files which have been persisted since the last heartbeat
   * @param options heartbeat options
   * @return the command for file system worker
   */
  public FileSystemCommand heartbeat(final long workerId,
      final List<Long> persistedFiles, final FileSystemHeartbeatPOptions options)
      throws IOException {
    return retryRPC(() -> mClient.fileSystemHeartbeat(FileSystemHeartbeatPRequest.newBuilder()
        .setWorkerId(workerId).addAllPersistedFiles(persistedFiles).setOptions(options).build())
        .getCommand());
  }
}
