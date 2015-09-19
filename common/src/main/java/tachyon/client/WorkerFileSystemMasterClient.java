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

package tachyon.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.MasterClientBase;
import tachyon.conf.TachyonConf;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.FileInfo;
import tachyon.thrift.FileSystemMasterService;
import tachyon.thrift.InvalidPathException;

/**
 * A wrapper for the thrift client to interact with the file system master, used by tachyon worker.
 * <p/>
 * Since thrift clients are not thread safe, this class is a wrapper to provide thread safety, and
 * to provide retries.
 */
public final class WorkerFileSystemMasterClient extends MasterClientBase {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private FileSystemMasterService.Client mClient = null;

  /**
   * Creates a new file system master client for the worker.
   *
   * @param masterAddress the master address
   * @param executorService the executor service
   * @param tachyonConf the Tachyon configuration
   */
  public WorkerFileSystemMasterClient(InetSocketAddress masterAddress,
      ExecutorService executorService, TachyonConf tachyonConf) {
    super(masterAddress, executorService, tachyonConf);
  }

  @Override
  protected String getServiceName() {
    return Constants.FILE_SYSTEM_MASTER_SERVICE_NAME;
  }

  @Override
  protected void afterConnect() {
    mClient = new FileSystemMasterService.Client(mProtocol);
  }

  /**
   * Adds a checkpoint.
   *
   * @param workerId the worker id
   * @param fileId the file id
   * @param length the checkpoint length
   * @param checkpointPath the checkpoint path
   * @return whether operation succeeded or not
   * @throws IOException if an I/O error occurs
   */
  public synchronized boolean addCheckpoint(final long workerId, final long fileId,
      final long length, final String checkpointPath) throws IOException {
    try {
      return retryRPC(new RpcCallableWithPropagateTException<Boolean>() {
        @Override
        public Boolean call() throws PropagateTException, TException {
          try {
            return mClient.addCheckpoint(workerId, fileId, length, checkpointPath);
          } catch (FileDoesNotExistException e) {
            throw new PropagateTException(e);
          }
        }
      });
    } catch (RpcCallableWithPropagateTException.PropagateTException e) {
      throw new IOException(e.getWrappedTException());
    }
  }

  /**
   * @param fileId the file id
   * @return the file info for the given file id
   * @throws FileDoesNotExistException if the file does not exist
   * @throws IOException if an I/O error occurs
   */
  // TODO(jiri): Factor this method out to a common client.
  public synchronized FileInfo getFileInfo(final long fileId) throws IOException,
      FileDoesNotExistException {
    try {
      return retryRPC(new RpcCallableWithPropagateTException<FileInfo>() {
        @Override
        public FileInfo call() throws PropagateTException, TException {
          try {
            return mClient.getFileInfo(fileId);
          } catch (FileDoesNotExistException e) {
            throw new PropagateTException(e);
          }
        }
      });
    } catch (RpcCallableWithPropagateTException.PropagateTException e) {
      throw (FileDoesNotExistException) e.getWrappedTException();
    }
  }

  /**
   * @return the set of pinned file ids
   * @throws IOException if an I/O error occurs
   */
  public synchronized Set<Long> getPinList() throws IOException {
    try {
      return retryRPC(new RpcCallableWithPropagateTException<Set<Long>>() {
        @Override
        public Set<Long> call() throws PropagateTException, TException {
          try {
            return mClient.workerGetPinIdList();
          } catch (InvalidPathException e) {
            throw new PropagateTException(e);
          }
        }
      });
    } catch (RpcCallableWithPropagateTException.PropagateTException e) {
      throw new IOException(e.getWrappedTException());
    }
  }
}
