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

package tachyon.client.file;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.thrift.TException;

import tachyon.Constants;
import tachyon.MasterClientBase;
import tachyon.TachyonURI;
import tachyon.client.file.options.CompleteFileOptions;
import tachyon.client.file.options.CreateOptions;
import tachyon.client.file.options.MkdirOptions;
import tachyon.client.file.options.SetAclOptions;
import tachyon.client.file.options.SetStateOptions;
import tachyon.conf.TachyonConf;
import tachyon.exception.ConnectionFailedException;
import tachyon.exception.TachyonException;
import tachyon.thrift.FileBlockInfo;
import tachyon.thrift.FileInfo;
import tachyon.thrift.FileSystemMasterClientService;
import tachyon.thrift.TachyonService;
import tachyon.thrift.TachyonTException;

/**
 * A wrapper for the thrift client to interact with the file system master, used by tachyon clients.
 *
 * Since thrift clients are not thread safe, this class is a wrapper to provide thread safety, and
 * to provide retries.
 */
@ThreadSafe
public final class FileSystemMasterClient extends MasterClientBase {
  private FileSystemMasterClientService.Client mClient = null;

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
  protected TachyonService.Client getClient() {
    return mClient;
  }

  @Override
  protected String getServiceName() {
    return Constants.FILE_SYSTEM_MASTER_CLIENT_SERVICE_NAME;
  }

  @Override
  protected long getServiceVersion() {
    return Constants.FILE_SYSTEM_MASTER_CLIENT_SERVICE_VERSION;
  }

  @Override
  protected void afterConnect() throws IOException {
    mClient = new FileSystemMasterClientService.Client(mProtocol);
  }

  /**
   * @param path the path
   * @return the file id for the given path, or -1 if the path does not point to a file
   * @throws ConnectionFailedException if network connection failed
   * @throws IOException if an I/O error occurs
   * @throws TachyonException if a Tachyon error occurs
   */
  public synchronized long getFileId(final String path)
      throws IOException, TachyonException {
    return retryRPC(new RpcCallableThrowsTachyonTException<Long>() {
      @Override
      public Long call() throws TachyonTException, TException {
        return mClient.getFileId(path);
      }
    });
  }

  /**
   * @param fileId the file id
   * @return the file info for the given file id
   * @throws IOException if an I/O error occurs
   * @throws TachyonException if a Tachyon error occurs
   */
  public synchronized FileInfo getFileInfo(final long fileId) throws IOException,
      TachyonException {
    return retryRPC(new RpcCallableThrowsTachyonTException<FileInfo>() {
      @Override
      public FileInfo call() throws TachyonTException, TException {
        return mClient.getFileInfo(fileId);
      }
    });
  }

  /**
   * @param fileId the file id
   * @return the list of file information for the given file id
   * @throws IOException if an I/O error occurs
   * @throws TachyonException if a Tachyon error occurs
   */
  public synchronized List<FileInfo> getFileInfoList(final long fileId) throws IOException,
      TachyonException {
    return retryRPC(new RpcCallableThrowsTachyonTException<List<FileInfo>>() {
      @Override
      public List<FileInfo> call() throws TachyonTException, TException {
        return mClient.getFileInfoList(fileId);
      }
    });
  }

  /**
   * @param fileId the file id
   * @param fileBlockIndex the file block index
   * @return the file block information
   * @throws IOException if an I/O error occurs
   * @throws TachyonException if a Tachyon error occurs
   */
  // TODO(calvin): Not sure if this is necessary.
  public synchronized FileBlockInfo getFileBlockInfo(final long fileId, final int fileBlockIndex)
      throws IOException, TachyonException {
    return retryRPC(new RpcCallableThrowsTachyonTException<FileBlockInfo>() {
      @Override
      public FileBlockInfo call() throws TachyonTException, TException {
        return mClient.getFileBlockInfo(fileId, fileBlockIndex);
      }
    });
  }

  /**
   * @param fileId the file id
   * @return the list of file block information for the given file id
   * @throws IOException if an I/O error occurs
   * @throws TachyonException if a Tachyon error occurs
   */
  // TODO(calvin): Not sure if this is necessary.
  public synchronized List<FileBlockInfo> getFileBlockInfoList(final long fileId)
      throws IOException, TachyonException {
    return retryRPC(new RpcCallableThrowsTachyonTException<List<FileBlockInfo>>() {
      @Override
      public List<FileBlockInfo> call() throws TachyonTException, TException {
        return mClient.getFileBlockInfoList(fileId);
      }
    });
  }

  /**
   * @param fileId the file id
   * @return a new block id for the given file id
   * @throws IOException if an I/O error occurs
   * @throws TachyonException if a Tachyon error occurs
   */
  public synchronized long getNewBlockIdForFile(final long fileId)
      throws IOException, TachyonException {
    return retryRPC(new RpcCallableThrowsTachyonTException<Long>() {
      @Override
      public Long call() throws TachyonTException, TException {
        return mClient.getNewBlockIdForFile(fileId);
      }
    });
  }

  /**
   * @return the under file system address
   * @throws ConnectionFailedException if network connection failed
   * @throws IOException if an I/O error occurs
   */
  public synchronized String getUfsAddress() throws IOException, ConnectionFailedException {
    return retryRPC(new RpcCallable<String>() {
      @Override
      public String call() throws TException {
        return mClient.getUfsAddress();
      }
    });
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
  public synchronized long create(final String path, final CreateOptions options)
      throws IOException, TachyonException {
    return retryRPC(new RpcCallableThrowsTachyonTException<Long>() {
      @Override
      public Long call() throws TachyonTException, TException {
        return mClient.create(path, options.toThrift());
      }
    });
  }

  /**
   * Marks a file as completed.
   *
   * @param fileId the file id
   * @param options the method options
   * @throws IOException if an I/O error occurs
   * @throws TachyonException if a Tachyon error occurs
   */
  public synchronized void completeFile(final long fileId, final CompleteFileOptions options)
      throws IOException, TachyonException {
    retryRPC(new RpcCallableThrowsTachyonTException<Void>() {
      @Override
      public Void call() throws TachyonTException, TException {
        mClient.completeFile(fileId, options.toThrift());
        return null;
      }
    });
  }

  /**
   * Deletes a file or a directory.
   *
   * @param id the id
   * @param recursive whether to delete the file recursively (when it is a directory)
   * @return whether operation succeeded or not
   * @throws IOException if an I/O error occurs
   * @throws TachyonException if a Tachyon error occurs
   */
  public synchronized boolean delete(final long id, final boolean recursive)
      throws IOException, TachyonException {
    return retryRPC(new RpcCallableThrowsTachyonTException<Boolean>() {
      @Override
      public Boolean call() throws TachyonTException, TException {
        return mClient.remove(id, recursive);
      }
    });
  }

  /**
   * Renames a file or a directory.
   *
   * @param id the id
   * @param dstPath new file path
   * @return whether operation succeeded or not
   * @throws IOException if an I/O error occurs
   * @throws TachyonException if a Tachyon error occurs
   */
  public synchronized boolean rename(final long id, final String dstPath)
      throws IOException, TachyonException {
    return retryRPC(new RpcCallableThrowsTachyonTException<Boolean>() {
      @Override
      public Boolean call() throws TachyonTException, TException {
        return mClient.rename(id, dstPath);
      }
    });
  }

  /**
   * Sets the file state.
   *
   * @param fileId the file id
   * @param options the file state options to be set
   * @throws IOException if an I/O error occurs
   * @throws TachyonException if a Tachyon error occurs
   */
  public synchronized void setState(final long fileId, final SetStateOptions options)
      throws IOException, TachyonException {
    retryRPC(new RpcCallableThrowsTachyonTException<Void>() {
      @Override
      public Void call() throws TachyonTException, TException {
        mClient.setState(fileId, options.toThrift());
        return null;
      }
    });
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
  public synchronized boolean mkdir(final String path, final MkdirOptions options)
      throws IOException, TachyonException {
    return retryRPC(new RpcCallableThrowsTachyonTException<Boolean>() {
      @Override
      public Boolean call() throws TachyonTException, TException {
        return mClient.mkdir(path, options.toThrift());
      }
    });
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
  public synchronized boolean free(final long fileId, final boolean recursive) throws IOException,
      TachyonException {
    return retryRPC(new RpcCallableThrowsTachyonTException<Boolean>() {
      @Override
      public Boolean call() throws TachyonTException, TException {
        return mClient.free(fileId, recursive);
      }
    });
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
  public synchronized long loadMetadata(final String path, final boolean recursive)
      throws IOException, TachyonException {
    return retryRPC(new RpcCallableThrowsTachyonTException<Long>() {
      @Override
      public Long call() throws TachyonTException, TException {
        return mClient.loadMetadata(path, recursive);
      }
    });
  }

  /**
   * Mounts the given UFS path under the given Tachyon path.
   *
   * @param tachyonPath the Tachyon path
   * @param ufsPath the UFS path
   * @return true if the given UFS path can be mounted, false otherwise
   * @throws TachyonException if a Tachyon error occurs
   * @throws IOException an I/O error occurs
   */
  public synchronized boolean mount(final TachyonURI tachyonPath, final TachyonURI ufsPath)
      throws TachyonException, IOException {
    return retryRPC(new RpcCallableThrowsTachyonTException<Boolean>() {
      @Override
      public Boolean call() throws TachyonTException, TException {
        return mClient.mount(tachyonPath.toString(), ufsPath.toString());
      }
    });
  }

  /**
   * Schedules the async persistence of the given file.
   *
   * @param fileId the file id
   * @throws TachyonException if a Tachyon error occurs
   * @throws IOException if an I/O error occurs
   */
  public synchronized void scheduleAsyncPersist(final long fileId)
      throws TachyonException, IOException {
    retryRPC(new RpcCallableThrowsTachyonTException<Void>() {
      @Override
      public Void call() throws TachyonTException, TException {
        mClient.scheduleAsyncPersist(fileId);
        return null;
      }
    });
  }

  /**
   * Sets the acl of a path.
   *
   * @param path the path of file or directory
   * @param options the acl option to be set
   * @return true if set successfully, false otherwise
   * @throws TachyonException if a Tachyon error occurs
   * @throws IOException an I/O error occurs
   */
  public synchronized void setAcl(final String path, final SetAclOptions options) throws
      TachyonException, IOException {
    retryRPC(new RpcCallableThrowsTachyonTException<Void>() {
      @Override
      public Void call() throws TachyonTException, TException {
        mClient.setAcl(path, options.toThrift());
        return null;
      }
    });
  }

  /**
   * Unmounts the given Tachyon path.
   *
   * @param tachyonPath the Tachyon path
   * @return true if the given Tachyon path can be unmounted, false otherwise
   * @throws TachyonException if a Tachyon error occurs
   * @throws IOException an I/O error occurs
   */
  public synchronized boolean unmount(final TachyonURI tachyonPath)
      throws TachyonException, IOException {
    return retryRPC(new RpcCallableThrowsTachyonTException<Boolean>() {
      @Override
      public Boolean call() throws TachyonTException, TException {
        return mClient.unmount(tachyonPath.toString());
      }
    });
  }
}
