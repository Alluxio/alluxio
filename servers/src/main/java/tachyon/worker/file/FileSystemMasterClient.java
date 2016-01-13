/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.worker.file;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.MasterClientBase;
import tachyon.conf.TachyonConf;
import tachyon.exception.ConnectionFailedException;
import tachyon.exception.TachyonException;
import tachyon.thrift.FileInfo;
import tachyon.thrift.FileSystemCommand;
import tachyon.thrift.FileSystemMasterWorkerService;
import tachyon.thrift.TachyonService;

/**
 * A wrapper for the thrift client to interact with the file system master, used by Tachyon worker.
 * <p/>
 * Since thrift clients are not thread safe, this class is a wrapper to provide thread safety, and
 * to provide retries.
 */
@ThreadSafe
public final class FileSystemMasterClient extends MasterClientBase {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private FileSystemMasterWorkerService.Client mClient = null;

  /**
   * Creates a instance of {@link FileSystemMasterClient}.
   *
   * @param masterAddress the master address
   * @param tachyonConf the Tachyon configuration
   */
  public FileSystemMasterClient(InetSocketAddress masterAddress, TachyonConf tachyonConf) {
    super(masterAddress, tachyonConf);
  }

  @Override
  protected TachyonService.Client getClient() {
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
   * @throws TachyonException if a Tachyon error occurs
   * @throws IOException if an I/O error occurs
   */
  public synchronized FileInfo getFileInfo(final long fileId) throws TachyonException, IOException {
    return retryRPC(new RpcCallableThrowsTachyonTException<FileInfo>() {
      @Override
      public FileInfo call() throws TException {
        return mClient.getFileInfo(fileId);
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
