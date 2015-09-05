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
import tachyon.retry.ExponentialBackoffRetry;
import tachyon.retry.RetryPolicy;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.DependencyDoesNotExistException;
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

  /**
   * Creates a new file system master client.
   *
   * @param masterAddress the master address
   * @param executorService the executor service
   * @param tachyonConf the Tachyon configuration
   */
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
  protected void afterDisconnect() {}

  private ExponentialBackoffRetry getRetryPolicy() {
    return new ExponentialBackoffRetry(10, Constants.SECOND_MS, 30);
  }

  /**
   * @param path the path
   * @return the file id for the given path
   * @throws IOException if an I/O error occurs
   */
  public synchronized long getFileId(String path) throws IOException, InvalidPathException {
    RetryPolicy retry = getRetryPolicy();
    while (true) {
      connect();
      try {
        return mClient.getFileId(path);
      } catch (InvalidPathException e) {
        LOG.error(e.getMessage(), e);
        throw e;
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        if (!retry.attemptRetry()) {
          throw new IOException(e.getMessage());
        }
        mConnected = false;
      }
    }
  }

  /**
   * @param fileId the file id
   * @return the file info for the given file id
   * @throws IOException if an I/O error occurs
   */
  public synchronized FileInfo getFileInfo(long fileId) throws IOException,
      FileDoesNotExistException {
    RetryPolicy retry = getRetryPolicy();
    while (true) {
      connect();
      try {
        return mClient.getFileInfo(fileId);
      } catch (FileDoesNotExistException e) {
        LOG.error(e.getMessage(), e);
        throw e;
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        if (!retry.attemptRetry()) {
          throw new IOException(e.getMessage());
        }
        mConnected = false;
      }
    }
  }

  /**
   * @param fileId the file id
   * @return the list of file information for the given file id
   * @throws IOException if an I/O error occurs
   */
  public synchronized List<FileInfo> getFileInfoList(long fileId) throws IOException,
      FileDoesNotExistException {
    RetryPolicy retry = getRetryPolicy();
    while (true) {
      connect();
      try {
        return mClient.getFileInfoList(fileId);
      } catch (FileDoesNotExistException e) {
        LOG.error(e.getMessage(), e);
        throw e;
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        if (!retry.attemptRetry()) {
          throw new IOException(e.getMessage());
        }
        mConnected = false;
      }
    }
  }

  /**
   * @param fileId the file id
   * @param fileBlockIndex the file block index
   * @return the file block information
   * @throws IOException if an I/O error occurs
   */
  // TODO: Not sure if this is necessary
  public synchronized FileBlockInfo getFileBlockInfo(long fileId, int fileBlockIndex)
      throws IOException, BlockInfoException, FileDoesNotExistException {
    RetryPolicy retry = getRetryPolicy();
    while (true) {
      connect();
      try {
        return mClient.getFileBlockInfo(fileId, fileBlockIndex);
      } catch (FileDoesNotExistException e) {
        LOG.error(e.getMessage(), e);
        throw e;
      } catch (BlockInfoException e) {
        LOG.error(e.getMessage(), e);
        throw e;
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        if (!retry.attemptRetry()) {
          throw new IOException(e.getMessage());
        }
        mConnected = false;
      }
    }
  }

  /**
   * @param fileId the file id
   * @return the list of file block information for the given file id
   * @throws IOException if an I/O error occurs
   */
  // TODO: Not sure if this is necessary
  public synchronized List<FileBlockInfo> getFileBlockInfoList(long fileId) throws IOException,
      FileDoesNotExistException {
    RetryPolicy retry = getRetryPolicy();
    while (true) {
      connect();
      try {
        return mClient.getFileBlockInfoList(fileId);
      } catch (FileDoesNotExistException e) {
        LOG.error(e.getMessage(), e);
        throw e;
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        if (!retry.attemptRetry()) {
          throw new IOException(e.getMessage());
        }
        mConnected = false;
      }
    }
  }

  /**
   * @param fileId the file id
   * @return a new block id for the given file id
   * @throws IOException if an I/O error occurs.
   */
  public synchronized long getNewBlockIdForFile(long fileId) throws IOException,
      FileDoesNotExistException {
    RetryPolicy retry = getRetryPolicy();
    while (true) {
      connect();
      try {
        return mClient.getNewBlockIdForFile(fileId);
      } catch (FileDoesNotExistException e) {
        LOG.error(e.getMessage(), e);
        throw e;
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        if (!retry.attemptRetry()) {
          throw new IOException(e.getMessage());
        }
        mConnected = false;
      }
    }
  }

  /**
   * @return the set of pinned file ids
   * @throws IOException if an I/O error occurs
   */
  public synchronized Set<Long> getPinList() throws IOException, InvalidPathException {
    RetryPolicy retry = getRetryPolicy();
    while (true) {
      connect();
      try {
        return mClient.workerGetPinIdList();
      } catch (InvalidPathException e) {
        LOG.error(e.getMessage(), e);
        throw e;
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        if (!retry.attemptRetry()) {
          throw new IOException(e.getMessage());
        }
        mConnected = false;
      }
    }
  }

  /**
   * @return the under file system address
   * @throws IOException if an I/O error occurs
   */
  public synchronized String getUfsAddress() throws IOException {
    RetryPolicy retry = getRetryPolicy();
    while (true) {
      connect();
      try {
        return mClient.getUfsAddress();
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        if (!retry.attemptRetry()) {
          throw new IOException(e.getMessage());
        }
        mConnected = false;
      }
    }
  }

  /**
   * Creates a new file.
   *
   * @param path the file path
   * @param blockSizeBytes the file size
   * @param recursive whether parent directories should be created if not present yet
   * @return the file id
   * @throws IOException if an I/O error occurs
   */
  public synchronized long createFile(String path, long blockSizeBytes, boolean recursive)
      throws IOException, BlockInfoException, InvalidPathException, FileAlreadyExistException {
    RetryPolicy retry = getRetryPolicy();
    while (true) {
      connect();
      try {
        return mClient.createFile(path, blockSizeBytes, recursive);
      } catch (BlockInfoException e) {
        LOG.error(e.getMessage(), e);
      } catch (InvalidPathException e) {
        LOG.error(e.getMessage(), e);
      } catch (FileAlreadyExistException e) {
        LOG.error(e.getMessage(), e);
        throw e;
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        if (!retry.attemptRetry()) {
          throw new IOException(e.getMessage());
        }
        mConnected = false;
      }
    }
  }

  /**
   * Loads a file from the under file system.
   *
   * @param path the file path
   * @param ufsPath the under file system path
   * @param blockSizeByte the file size
   * @param recursive whether parent directories should be loaded if not present yet
   * @return the file id
   * @throws IOException if an I/O error occurs
   */
  public synchronized long loadFileFromUfs(String path, String ufsPath, long blockSizeByte,
      boolean recursive) throws IOException, FileDoesNotExistException {
    RetryPolicy retry = getRetryPolicy();
    while (true) {
      connect();
      try {
        return mClient.loadFileFromUfs(path, ufsPath, blockSizeByte, recursive);
      } catch (FileDoesNotExistException e) {
        LOG.error(e.getMessage(), e);
        throw e;
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        if (!retry.attemptRetry()) {
          throw new IOException(e.getMessage());
        }
        mConnected = false;
      }
    }
  }

  /**
   * Marks a file as completed.
   *
   * @param fileId the file id
   * @throws IOException if an I/O error occurs
   */
  public synchronized void completeFile(long fileId) throws IOException, FileDoesNotExistException,
      SuspectedFileSizeException, BlockInfoException {
    RetryPolicy retry = getRetryPolicy();
    while (true) {
      connect();
      try {
        mClient.completeFile(fileId);
        return;
      } catch (FileDoesNotExistException e) {
        LOG.error(e.getMessage(), e);
      } catch (SuspectedFileSizeException e) {
        LOG.error(e.getMessage(), e);
      } catch (BlockInfoException e) {
        LOG.error(e.getMessage(), e);
        throw e;
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        if (!retry.attemptRetry()) {
          throw new IOException(e.getMessage());
        }
        mConnected = false;
      }
    }
  }

  /**
   * Deletes a file.
   *
   * @param fileId the file id
   * @param recursive whether to delete the file recursively (when it is a directory)
   * @return whether operation succeeded or not
   * @throws IOException  if an I/O error occurs
   */
  public synchronized boolean deleteFile(long fileId, boolean recursive) throws IOException {
    RetryPolicy retry = getRetryPolicy();
    while (true) {
      connect();
      try {
        return mClient.deleteFile(fileId, recursive);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        if (!retry.attemptRetry()) {
          throw new IOException(e.getMessage());
        }
        mConnected = false;
      }
    }
  }

  /**
   * Renames a file.
   *
   * @param fileId the file id
   * @param dstPath new file path
   * @return whether operation succeeded or not
   * @throws IOException if an I/O error occurs
   */
  public synchronized boolean renameFile(long fileId, String dstPath) throws IOException,
      FileDoesNotExistException {
    RetryPolicy retry = getRetryPolicy();
    while (true) {
      connect();
      try {
        return mClient.renameFile(fileId, dstPath);
      } catch (FileDoesNotExistException e) {
        LOG.error(e.getMessage(), e);
        throw e;
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        if (!retry.attemptRetry()) {
          throw new IOException(e.getMessage());
        }
        mConnected = false;
      }
    }
  }

  /**
   * Sets the "pinned" status for a file.
   *
   * @param fileId the file id
   * @param pinned the pinned status to use
   * @throws IOException if an I/O error occurs
   */
  public synchronized void setPinned(long fileId, boolean pinned) throws IOException,
      FileDoesNotExistException {
    RetryPolicy retry = getRetryPolicy();
    while (true) {
      connect();
      try {
        mClient.setPinned(fileId, pinned);
        return;
      } catch (FileDoesNotExistException e) {
        LOG.error(e.getMessage(), e);
        throw e;
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        if (!retry.attemptRetry()) {
          throw new IOException(e.getMessage());
        }
        mConnected = false;
      }
    }
  }

  /**
   * Creates a new directory.
   *
   * @param path the directory path
   * @param recursive whether parent directories should be created if they don't exist yet
   * @return whether operation succeeded or not
   * @throws IOException if an I/O error occurs
   */
  public synchronized boolean createDirectory(String path, boolean recursive) throws IOException,
      FileAlreadyExistException, InvalidPathException {
    RetryPolicy retry = getRetryPolicy();
    while (true) {
      connect();
      try {
        return mClient.createDirectory(path, recursive);
      } catch (FileAlreadyExistException e) {
        LOG.error(e.getMessage(), e);
        throw e;
      } catch (InvalidPathException e) {
        LOG.error(e.getMessage(), e);
        throw e;
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        if (!retry.attemptRetry()) {
          throw new IOException(e.getMessage());
        }
        mConnected = false;
      }
    }
  }

  /**
   * Frees a file.
   *
   * @param fileId the file id
   * @param recursive whether free the file recursively (when it is a directory)
   * @return whether operation succeeded or not
   * @throws IOException if an I/O error occurs
   */
  public synchronized boolean free(long fileId, boolean recursive) throws IOException,
      FileDoesNotExistException {
    RetryPolicy retry = getRetryPolicy();
    while (true) {
      connect();
      try {
        return mClient.free(fileId, recursive);
      } catch (FileDoesNotExistException e) {
        LOG.error(e.getMessage(), e);
        throw e;
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        if (!retry.attemptRetry()) {
          throw new IOException(e.getMessage());
        }
        mConnected = false;
      }
    }
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
  public synchronized boolean addCheckpoint(long workerId, long fileId, long length,
      String checkpointPath) throws IOException, FileDoesNotExistException {
    RetryPolicy retry = getRetryPolicy();
    while (true) {
      connect();
      try {
        return mClient.addCheckpoint(workerId, fileId, length, checkpointPath);
      } catch (FileDoesNotExistException e) {
        LOG.error(e.getMessage(), e);
        throw e;
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        if (!retry.attemptRetry()) {
          throw new IOException(e.getMessage());
        }
        mConnected = false;
      }
    }
  }

  /**
   * Reports a lost file.
   *
   * @param fileId the file id
   * @throws IOException if an I/O error occurs
   */
  public synchronized void reportLostFile(long fileId) throws IOException {
    while (!mIsClosed) {
      connect();
      try {
        mClient.reportLostFile(fileId);
      } catch (FileDoesNotExistException e) {
        throw new IOException(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    throw new IOException("This connection has been closed.");
  }

  /**
   * Requests files in a dependency.
   *
   * @param depId the dependency id
   * @throws IOException if an I/O error occurs
   */
  public synchronized void requestFilesInDependency(int depId) throws IOException {
    while (!mIsClosed) {
      connect();
      try {
        mClient.requestFilesInDependency(depId);
      } catch (DependencyDoesNotExistException e) {
        throw new IOException(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    throw new IOException("This connection has been closed.");
  }

  // TODO: See if these methods can/should be implemented

  /**
   * Sends a heartbeat message.
   *
   * Not implemented.
   *
   * @throws IOException if an I/O error occurs
   */
  public synchronized void userHeartbeat() throws IOException {}

  /**
   * Creates a dependency.
   *
   * Not implemented.
   *
   * @param parents the dependency parents
   * @param children the dependency children
   * @param commandPrefix the prefix of the dependency command
   * @param data the dependency data
   * @param comment a comment
   * @param framework the framework
   * @param frameworkVersion the framework version
   * @param dependencyType the dependency type
   * @param childrenBlockSizeByte the children block size (in bytes)
   * @return the dependency id
   * @throws IOException if an I/O error occurs
   */
  public synchronized int user_createDependency(List<String> parents, List<String> children,
      String commandPrefix, List<ByteBuffer> data, String comment, String framework,
      String frameworkVersion, int dependencyType, long childrenBlockSizeByte) throws IOException {
    return -1;
  }

  /**
   * Gets dependency information for a dependency.
   *
   * Not implemented.
   *
   * @param dependencyId the dependency id
   * @return the dependency information
   * @throws IOException if an I/O error occurs
   */
  public synchronized DependencyInfo getDependencyInfo(int dependencyId) throws IOException {
    return null;
  }
}
