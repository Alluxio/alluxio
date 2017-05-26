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
import alluxio.thrift.AlluxioService;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.FileSystemCommand;
import alluxio.thrift.FileSystemHeartbeatTOptions;
import alluxio.thrift.FileSystemMasterWorkerService;
import alluxio.thrift.GetFileInfoTOptions;
import alluxio.thrift.GetPinnedFileIdsTOptions;
import alluxio.thrift.GetUfsInfoTOptions;
import alluxio.thrift.UfsInfo;
import alluxio.wire.FileInfo;
import alluxio.wire.ThriftUtils;

import org.apache.thrift.TException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A wrapper for the thrift client to interact with the file system master, used by Alluxio worker.
 * <p/>
 * Since thrift clients are not thread safe, this class is a wrapper to provide thread safety, and
 * to provide retries.
 */
@ThreadSafe
public final class FileSystemMasterClient extends AbstractMasterClient {
  private FileSystemMasterWorkerService.Client mClient = null;

  /**
   * Creates a instance of {@link FileSystemMasterClient}.
   *
   * @param masterAddress the master address
   */
  public FileSystemMasterClient(InetSocketAddress masterAddress) {
    super(null, masterAddress);
  }

  @Override
  protected AlluxioService.Client getClient() {
    return mClient;
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
    mClient = new FileSystemMasterWorkerService.Client(mProtocol);
  }

  /**
   * @param fileId the id of the file for which to get the {@link FileInfo}
   * @return the file info for the given file id
   */
  public synchronized FileInfo getFileInfo(final long fileId) throws IOException {
    return retryRPC(new RpcCallable<FileInfo>() {
      @Override
      public FileInfo call() throws TException {
        return ThriftUtils
            .fromThrift(mClient.getFileInfo(fileId, new GetFileInfoTOptions()).getFileInfo());
      }
    });
  }

  /**
   * @return the set of pinned file ids
   */
  public synchronized Set<Long> getPinList() throws IOException {
    return retryRPC(new RpcCallable<Set<Long>>() {
      @Override
      public Set<Long> call() throws TException {
        return mClient.getPinnedFileIds(new GetPinnedFileIdsTOptions()).getPinnedFileIds();
      }
    });
  }

  /**
   * @param mountId the id of the mount
   * @return the ufs information for the give ufs
   * @throws IOException if an I/O error occurs
   */
  public synchronized UfsInfo getUfsInfo(final long mountId) throws IOException {
    return retryRPC(new RpcCallable<UfsInfo>() {
      @Override
      public UfsInfo call() throws TException {
        return mClient.getUfsInfo(mountId, new GetUfsInfoTOptions()).getUfsInfo();
      }
    });
  }

  /**
   * Heartbeats to the worker. It also carries command for the worker to persist the given files.
   *
   * @param workerId the id of the worker that heartbeats
   * @param persistedFiles the files which have been persisted since the last heartbeat
   * @return the command for file system worker
   */
  public synchronized FileSystemCommand heartbeat(final long workerId,
      final List<Long> persistedFiles) throws IOException {
    return retryRPC(new RpcCallable<FileSystemCommand>() {
      @Override
      public FileSystemCommand call() throws AlluxioTException, TException {
        return mClient
            .fileSystemHeartbeat(workerId, persistedFiles, new FileSystemHeartbeatTOptions())
            .getCommand();
      }
    });
  }
}
