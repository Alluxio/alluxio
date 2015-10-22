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

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.MasterClientBase;
import tachyon.conf.TachyonConf;
import tachyon.thrift.FileInfo;
import tachyon.thrift.FileSystemMasterService;
import tachyon.thrift.TachyonTException;

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
   * @param tachyonConf the Tachyon configuration
   */
  public WorkerFileSystemMasterClient(InetSocketAddress masterAddress, TachyonConf tachyonConf) {
    super(masterAddress, tachyonConf);
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
   * Persists a file.
   *
   * @param fileId the file id
   * @param length the checkpoint length
   * @return whether operation succeeded or not
   * @throws IOException if an I/O error occurs
   */
  public synchronized boolean persistFile(long fileId, long length) throws IOException {
    int retry = 0;
    while (!mClosed && (retry ++) <= RPC_MAX_NUM_RETRY) {
      connect();
      try {
        return mClient.persistFile(fileId, length);
      } catch (TachyonTException e) {
        throw new IOException(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    throw new IOException("Failed after " + retry + " retries.");
  }

  /**
   * @param fileId the file id
   * @return the file info for the given file id
   * @throws IOException if an I/O error occurs
   */
  // TODO(jiri): Factor this method out to a common client.
  public synchronized FileInfo getFileInfo(long fileId) throws IOException {
    int retry = 0;
    while (!mClosed && (retry ++) <= RPC_MAX_NUM_RETRY) {
      connect();
      try {
        return mClient.getFileInfo(fileId);
      } catch (TachyonTException e) {
        throw new IOException(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    throw new IOException("Failed after " + retry + " retries.");
  }

  /**
   * @return the set of pinned file ids
   * @throws IOException if an I/O error occurs
   */
  public synchronized Set<Long> getPinList() throws IOException {
    int retry = 0;
    while (!mClosed && (retry ++) <= RPC_MAX_NUM_RETRY) {
      connect();
      try {
        return mClient.workerGetPinIdList();
      } catch (TachyonTException e) {
        throw new IOException(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    throw new IOException("Failed after " + retry + " retries.");
  }
}
