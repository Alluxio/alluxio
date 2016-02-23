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

import alluxio.AbstractMasterClient;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.exception.AlluxioException;
import alluxio.exception.ConnectionFailedException;
import alluxio.thrift.AlluxioService;
import alluxio.thrift.FileSystemCommand;
import alluxio.thrift.FileSystemMasterWorkerService;
import alluxio.wire.FileInfo;
import alluxio.wire.ThriftUtils;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private FileSystemMasterWorkerService.Client mClient = null;

  /**
   * Creates a instance of {@link FileSystemMasterClient}.
   *
   * @param masterAddress the master address
   * @param configuration the Alluxio configuration
   */
  public FileSystemMasterClient(InetSocketAddress masterAddress, Configuration configuration) {
    super(masterAddress, configuration);
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
   * @throws AlluxioException if an Alluxio error occurs
   * @throws IOException if an I/O error occurs
   */
  public synchronized FileInfo getFileInfo(final long fileId) throws AlluxioException, IOException {
    return retryRPC(new RpcCallableThrowsAlluxioTException<FileInfo>() {
      @Override
      public FileInfo call() throws TException {
        return ThriftUtils.fromThrift(mClient.getFileInfo(fileId));
      }
    });
  }

  /**
   * @return the set of pinned file ids
   * @throws ConnectionFailedException if network connection failed
   * @throws IOException if an I/O error occurs
   */
  public synchronized Set<Long> getPinList() throws ConnectionFailedException, IOException {
    return retryRPC(new RpcCallable<Set<Long>>() {
      @Override
      public Set<Long> call() throws TException {
        return mClient.getPinIdList();
      }
    });
  }

  /**
   * Heartbeats to the worker. It also carries command for the worker to persist the given files.
   *
   * @param workerId the id of the worker that heartbeats
   * @param persistedFiles the files which have been persisted since the last heartbeat
   * @return the command for file system worker
   * @throws IOException if file persistence fails
   * @throws ConnectionFailedException if network connection failed
   */
  public synchronized FileSystemCommand heartbeat(final long workerId,
      final List<Long> persistedFiles) throws ConnectionFailedException, IOException {
    return retryRPC(new RpcCallable<FileSystemCommand>() {
      @Override
      public FileSystemCommand call() throws TException {
        return mClient.heartbeat(workerId, persistedFiles);
      }
    });
  }
}
