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
import java.util.List;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.MasterClientBase;
import tachyon.TachyonURI;
import tachyon.client.file.options.CreateOptions;
import tachyon.client.file.options.MkdirOptions;
import tachyon.conf.TachyonConf;
import tachyon.exception.TachyonException;
import tachyon.thrift.FileBlockInfo;
import tachyon.thrift.FileInfo;
import tachyon.thrift.FileSystemMasterService;
import tachyon.thrift.TachyonTException;

/**
 * A wrapper for the thrift client to interact with the file system master, used by tachyon clients.
 *
 * Since thrift clients are not thread safe, this class is a wrapper to provide thread safety, and
 * to provide retries.
 */
// TODO(gene): Figure out a retry utility to make all the retry logic in this file better.
public final class FileSystemMasterClient extends MasterClientBase {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private FileSystemMasterService.Client mClient = null;

  /**
   * Creates a new file system master client.
   *
   * @param masterAddress the master address
   * @param tachyonConf the Tachyon configuration
   */
  public FileSystemMasterClient(InetSocketAddress masterAddress, TachyonConf tachyonConf) {
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
   * @param path the path
   * @return the file id for the given path, or -1 if the path does not point to a file
   * @throws IOException if an I/O error occurs
   */
  public synchronized long getFileId(String path) throws IOException {
    int retry = 0;
    while (!mClosed && (retry ++) <= RPC_MAX_NUM_RETRY) {
      connect();
      try {
        return mClient.getFileId(path);
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
   * @throws TachyonException if a Tachyon error occurs
   */
  public synchronized FileInfo getFileInfo(long fileId) throws IOException,
      TachyonException {
    int retry = 0;
    while (!mClosed && (retry ++) <= RPC_MAX_NUM_RETRY) {
      connect();
      try {
        return mClient.getFileInfo(fileId);
      } catch (TachyonTException e) {
        throw TachyonException.from(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    throw new IOException("Failed after " + retry + " retries.");
  }

  /**
   * @param fileId the file id
   * @return the list of file information for the given file id
   * @throws IOException if an I/O error occurs
   * @throws TachyonException if a Tachyon error occurs
   */
  public synchronized List<FileInfo> getFileInfoList(long fileId) throws IOException,
      TachyonException {
    int retry = 0;
    while (!mClosed && (retry ++) <= RPC_MAX_NUM_RETRY) {
      connect();
      try {
        return mClient.getFileInfoList(fileId);
      } catch (TachyonTException e) {
        throw TachyonException.from(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    throw new IOException("Failed after " + retry + " retries.");
  }

  /**
   * @param fileId the file id
   * @param fileBlockIndex the file block index
   * @return the file block information
   * @throws IOException if an I/O error occurs
   * @throws TachyonException if a Tachyon error occurs
   */
  // TODO(calvin): Not sure if this is necessary.
  public synchronized FileBlockInfo getFileBlockInfo(long fileId, int fileBlockIndex)
      throws IOException, TachyonException {
    int retry = 0;
    while (!mClosed && (retry ++) <= RPC_MAX_NUM_RETRY) {
      connect();
      try {
        return mClient.getFileBlockInfo(fileId, fileBlockIndex);
      } catch (TachyonTException e) {
        throw TachyonException.from(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    throw new IOException("Failed after " + retry + " retries.");
  }

  /**
   * @param fileId the file id
   * @return the list of file block information for the given file id
   * @throws IOException if an I/O error occurs
   * @throws TachyonException if a Tachyon error occurs
   */
  // TODO(calvin): Not sure if this is necessary.
  public synchronized List<FileBlockInfo> getFileBlockInfoList(long fileId) throws IOException,
      TachyonException {
    int retry = 0;
    while (!mClosed && (retry ++) <= RPC_MAX_NUM_RETRY) {
      connect();
      try {
        return mClient.getFileBlockInfoList(fileId);
      } catch (TachyonTException e) {
        throw TachyonException.from(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    throw new IOException("Failed after " + retry + " retries.");
  }

  /**
   * @param fileId the file id
   * @return a new block id for the given file id
   * @throws IOException if an I/O error occurs
   * @throws TachyonException if a Tachyon error occurs
   */
  public synchronized long getNewBlockIdForFile(long fileId) throws IOException, TachyonException {
    int retry = 0;
    while (!mClosed && (retry ++) <= RPC_MAX_NUM_RETRY) {
      connect();
      try {
        return mClient.getNewBlockIdForFile(fileId);
      } catch (TachyonTException e) {
        throw TachyonException.from(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    throw new IOException("Failed after " + retry + " retries.");
  }

  /**
   * @return the under file system address
   * @throws IOException if an I/O error occurs
   */
  public synchronized String getUfsAddress() throws IOException {
    int retry = 0;
    while (!mClosed && (retry ++) <= RPC_MAX_NUM_RETRY) {
      connect();
      try {
        return mClient.getUfsAddress();
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    throw new IOException("Failed after " + retry + " retries.");
  }

  /**
   * Creates a new file.
   *
   * @param path the file path
   * @param options method options
   * @return the file id
   * @throws IOException if an I/O error occurs
   * @throws TachyonException if a Tachyon error occurs
   */
  public synchronized long create(String path, CreateOptions options) throws IOException,
      TachyonException {
    int retry = 0;
    while (!mClosed && (retry ++) <= RPC_MAX_NUM_RETRY) {
      connect();
      try {
        return mClient.create(path, options.toThrift());
      } catch (TachyonTException e) {
        throw TachyonException.from(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    throw new IOException("Failed after " + retry + " retries.");
  }

  /**
   * Marks a file as completed.
   *
   * @param fileId the file id
   * @throws IOException if an I/O error occurs
   * @throws TachyonException if a Tachyon error occurs
   */
  public synchronized void completeFile(long fileId) throws IOException, TachyonException {
    int retry = 0;
    while (!mClosed && (retry ++) <= RPC_MAX_NUM_RETRY) {
      connect();
      try {
        mClient.completeFile(fileId);
        return;
      } catch (TachyonTException e) {
        throw TachyonException.from(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    throw new IOException("Failed after " + retry + " retries.");
  }

  /**
   * Deletes a file.
   *
   * @param fileId the file id
   * @param recursive whether to delete the file recursively (when it is a directory)
   * @return whether operation succeeded or not
   * @throws IOException if an I/O error occurs
   * @throws TachyonException if a Tachyon error occurs
   */
  public synchronized boolean deleteFile(long fileId, boolean recursive) throws IOException,
      TachyonException {
    int retry = 0;
    while (!mClosed && (retry ++) <= RPC_MAX_NUM_RETRY) {
      connect();
      try {
        return mClient.deleteFile(fileId, recursive);
      } catch (TachyonTException e) {
        throw TachyonException.from(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    throw new IOException("Failed after " + retry + " retries.");
  }

  /**
   * Renames a file.
   *
   * @param fileId the file id
   * @param dstPath new file path
   * @return whether operation succeeded or not
   * @throws IOException if an I/O error occurs
   * @throws TachyonException if a Tachyon error occurs
   */
  public synchronized boolean renameFile(long fileId, String dstPath) throws IOException,
      TachyonException {
    int retry = 0;
    while (!mClosed && (retry ++) <= RPC_MAX_NUM_RETRY) {
      connect();
      try {
        return mClient.renameFile(fileId, dstPath);
      } catch (TachyonTException e) {
        throw TachyonException.from(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    throw new IOException("Failed after " + retry + " retries.");
  }

  /**
   * Sets the "pinned" status for a file.
   *
   * @param fileId the file id
   * @param pinned the pinned status to use
   * @throws IOException if an I/O error occurs
   * @throws TachyonException if a Tachyon error occurs
   */
  public synchronized void setPinned(long fileId, boolean pinned) throws IOException,
      TachyonException {
    int retry = 0;
    while (!mClosed && (retry ++) <= RPC_MAX_NUM_RETRY) {
      connect();
      try {
        mClient.setPinned(fileId, pinned);
        return;
      } catch (TachyonTException e) {
        throw TachyonException.from(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    throw new IOException("Failed after " + retry + " retries.");
  }

  /**
   * Creates a new directory.
   *
   * @param path the directory path
   * @param options method options
   * @return whether operation succeeded or not
   * @throws IOException if an I/O error occurs
   * @throws TachyonException if a Tachyon error occurs
   */
  public synchronized boolean mkdir(String path, MkdirOptions options) throws IOException,
      TachyonException {
    int retry = 0;
    while (!mClosed && (retry ++) <= RPC_MAX_NUM_RETRY) {
      connect();
      try {
        return mClient.mkdir(path, options.toThrift());
      } catch (TachyonTException e) {
        throw TachyonException.from(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    throw new IOException("Failed after " + retry + " retries.");
  }

  /**
   * Frees a file.
   *
   * @param fileId the file id
   * @param recursive whether free the file recursively (when it is a directory)
   * @return whether operation succeeded or not
   * @throws IOException if an I/O error occurs
   * @throws TachyonException if a Tachyon error occurs
   */
  public synchronized boolean free(long fileId, boolean recursive) throws IOException,
      TachyonException {
    int retry = 0;
    while (!mClosed && (retry ++) <= RPC_MAX_NUM_RETRY) {
      connect();
      try {
        return mClient.free(fileId, recursive);
      } catch (TachyonTException e) {
        throw TachyonException.from(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    throw new IOException("Failed after " + retry + " retries.");
  }

  /**
   * Reports a lost file.
   *
   * @param fileId the file id
   * @throws IOException if an I/O error occurs
   * @throws TachyonException if a Tachyon error occurs
   */
  public synchronized void reportLostFile(long fileId) throws IOException, TachyonException {
    int retry = 0;
    while (!mClosed && (retry ++) <= RPC_MAX_NUM_RETRY) {
      connect();
      try {
        mClient.reportLostFile(fileId);
      } catch (TachyonTException e) {
        throw TachyonException.from(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    throw new IOException("Failed after " + retry + " retries.");
  }

  /**
   * Loads a file from the under file system.
   *
   * @param path the Tachyon path of the file
   * @param recursive whether parent directories should be loaded if not present yet
   * @return the file id
   * @throws TachyonException if a tachyon error occurs
   * @throws IOException if an I/O error occurs
   */
  public synchronized long loadMetadata(String path, boolean recursive)
      throws IOException, TachyonException {
    int retry = 0;
    while (!mClosed && (retry ++) <= RPC_MAX_NUM_RETRY) {
      connect();
      try {
        return mClient.loadMetadata(path, recursive);
      } catch (TachyonTException e) {
        throw TachyonException.from(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    throw new IOException("Failed after " + retry + " retries.");
  }

  /**
   * Mounts the given UFS path under the given Tachyon path.
   *
   * @param tachyonPath the Tachyon path
   * @param ufsPath the UFS path
   * @throws IOException an I/O error occurs
   */
  public synchronized boolean mount(TachyonURI tachyonPath, TachyonURI ufsPath) throws IOException {
    int retry = 0;
    while (!mClosed && (retry ++) <= RPC_MAX_NUM_RETRY) {
      connect();
      try {
        return mClient.mount(tachyonPath.toString(), ufsPath.toString());
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    throw new IOException("Failed after " + retry + " retries.");
  }

  /**
   * Unmounts the given Tachyon path.
   *
   * @param tachyonPath the Tachyon path
   * @throws IOException an I/O error occurs
   */
  public synchronized boolean unmount(TachyonURI tachyonPath) throws IOException {
    int retry = 0;
    while (!mClosed && (retry ++) <= RPC_MAX_NUM_RETRY) {
      connect();
      try {
        return mClient.unmount(tachyonPath.toString());
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    throw new IOException("Failed after " + retry + " retries.");
  }
}
