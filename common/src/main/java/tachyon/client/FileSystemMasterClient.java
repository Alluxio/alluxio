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
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.MasterClientBase;
import tachyon.conf.TachyonConf;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.DependencyInfo;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileBlockInfo;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.FileInfo;
import tachyon.thrift.FileSystemMasterService;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.SuspectedFileSizeException;

/**
 * The FileSystemMaster client, for clients.
 *
 * Since thrift clients are not thread safe, this class is a wrapper to provide thread safety.
 */
// TODO: split out worker-specific calls to a fs master client for workers.
public final class FileSystemMasterClient extends MasterClientBase {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private FileSystemMasterService.Client mClient = null;

  public FileSystemMasterClient(InetSocketAddress masterAddress, ExecutorService executorService,
      TachyonConf tachyonConf) {
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

  @Override
  protected void afterDisconnect() {
  }

  public synchronized long getFileId(String path) throws IOException {
    while (!mIsClosed) {
      connect();
      try {
        return mClient.getFileId(path);
      } catch (InvalidPathException e) {
        throw new IOException(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    throw new IOException("This connection has been closed.");
  }

  public synchronized FileInfo getFileInfo(long fileId) throws IOException {
    while (!mIsClosed) {
      connect();
      try {
        return mClient.getFileInfo(fileId);
      } catch (FileDoesNotExistException e) {
        throw new IOException(e);
      } catch (InvalidPathException e) {
        throw new IOException(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    throw new IOException("This connection has been closed.");
  }

  public synchronized List<FileInfo> getFileInfoList(long fileId) throws IOException {
    while (!mIsClosed) {
      connect();
      try {
        return mClient.getFileInfoList(fileId);
      } catch (FileDoesNotExistException e) {
        throw new IOException(e);
      } catch (InvalidPathException e) {
        throw new IOException(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    throw new IOException("This connection has been closed.");
  }

  // TODO: Not sure if this is necessary
  public synchronized FileBlockInfo getFileBlockInfo(long fileId, int fileBlockIndex)
      throws IOException {
    while (!mIsClosed) {
      connect();
      try {
        return mClient.getFileBlockInfo(fileId, fileBlockIndex);
      } catch (FileDoesNotExistException e) {
        throw new IOException(e);
      } catch (BlockInfoException e) {
        throw new IOException(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    throw new IOException("This connection has been closed.");
  }

  // TODO: Not sure if this is necessary
  public synchronized List<FileBlockInfo> getFileBlockInfoList(long fileId) throws IOException {
    while (!mIsClosed) {
      connect();
      try {
        return mClient.getFileBlockInfoList(fileId);
      } catch (FileDoesNotExistException e) {
        throw new IOException(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    throw new IOException("This connection has been closed.");
  }

  public synchronized long getNewBlockIdForFile(long fileId) throws IOException {
    while (!mIsClosed) {
      connect();
      try {
        return mClient.getNewBlockIdForFile(fileId);
      } catch (FileDoesNotExistException e) {
        throw new IOException(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    throw new IOException("This connection has been closed.");
  }

  public synchronized Set<Long> getPinList() throws IOException {
    while (!mIsClosed) {
      connect();
      try {
        return mClient.workerGetPinIdList();
      } catch (InvalidPathException e) {
        throw new IOException(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    throw new IOException("This connection has been closed.");
  }

  public synchronized String getUfsAddress() throws IOException {
    while (!mIsClosed) {
      connect();
      try {
        return mClient.getUfsAddress();
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    throw new IOException("This connection has been closed.");
  }

  public synchronized long createFile(String path, long blockSizeBytes, boolean recursive)
      throws IOException {
    while (!mIsClosed) {
      connect();
      try {
        return mClient.createFile(path, blockSizeBytes, recursive);
      } catch (BlockInfoException e) {
        throw new IOException(e);
      } catch (InvalidPathException e) {
        throw new IOException(e);
      } catch (FileAlreadyExistException e) {
        throw new IOException(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    throw new IOException("This connection has been closed.");
  }

  public synchronized long loadFileFromUfs(String path, String ufsPath, long blockSizeByte,
      boolean recursive) throws IOException {
    while (!mIsClosed) {
      connect();
      try {
        return mClient.loadFileFromUfs(path, ufsPath, blockSizeByte, recursive);
      } catch (FileDoesNotExistException e) {
        throw new IOException(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    throw new IOException("This connection has been closed.");
  }

  public synchronized void completeFile(long fileId) throws IOException {
    while (!mIsClosed) {
      connect();
      try {
        mClient.completeFile(fileId);
        return;
      } catch (FileDoesNotExistException e) {
        throw new IOException(e);
      } catch (SuspectedFileSizeException e) {
        throw new IOException(e);
      } catch (BlockInfoException e) {
        throw new IOException(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    throw new IOException("This connection has been closed.");
  }

  public synchronized boolean deleteFile(long fileId, boolean recursive) throws IOException {
    while (!mIsClosed) {
      connect();
      try {
        return mClient.deleteFile(fileId, recursive);
      } catch (FileDoesNotExistException e) {
        throw new IOException(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    throw new IOException("This connection has been closed.");
  }

  public synchronized boolean renameFile(long fileId, String dstPath) throws IOException {
    while (!mIsClosed) {
      connect();
      try {
        return mClient.renameFile(fileId, dstPath);
      } catch (FileDoesNotExistException e) {
        throw new IOException(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    throw new IOException("This connection has been closed.");
  }

  public synchronized void setPinned(long fileId, boolean pinned) throws IOException {
    while (!mIsClosed) {
      connect();
      try {
        mClient.setPinned(fileId, pinned);
        return;
      } catch (FileDoesNotExistException e) {
        throw new IOException(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    throw new IOException("This connection has been closed.");
  }

  public synchronized boolean createDirectory(String path, boolean recursive) throws IOException {
    while (!mIsClosed) {
      connect();
      try {
        return mClient.createDirectory(path, recursive);
      } catch (FileDoesNotExistException e) {
        throw new IOException(e);
      } catch (InvalidPathException e) {
        throw new IOException(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    throw new IOException("This connection has been closed.");
  }

  public synchronized boolean free(long fileId, boolean recursive) throws IOException {
    while (!mIsClosed) {
      connect();
      try {
        return mClient.free(fileId, recursive);
      } catch (FileDoesNotExistException e) {
        throw new IOException(e);
      } catch (InvalidPathException e) {
        throw new IOException(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    throw new IOException("This connection has been closed.");
  }

  public synchronized boolean addCheckpoint(long workerId, long fileId, long length,
      String checkpointPath) throws IOException {
    while (!mIsClosed) {
      connect();
      try {
        return mClient.addCheckpoint(workerId, fileId, length, checkpointPath);
      } catch (FileDoesNotExistException e) {
        throw new IOException(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    throw new IOException("This connection has been closed.");
  }

  // TODO: See if these methods can/should be implemented

  public synchronized void userHeartbeat() throws IOException {

  }

  public synchronized int user_createDependency(List<String> parents, List<String> children,
      String commandPrefix, List<ByteBuffer> data, String comment, String framework,
      String frameworkVersion, int dependencyType, long childrenBlockSizeByte) throws IOException {
    return -1;
  }

  public synchronized DependencyInfo getDependencyInfo(int dependencyId) throws IOException {
    return null;
  }

  public synchronized void reportLostFile(long fileId) throws IOException {

  }

  public synchronized void requestFilesInDependency(int depId) throws IOException {

  }
}
