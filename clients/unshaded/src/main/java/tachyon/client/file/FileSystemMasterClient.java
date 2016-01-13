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

import org.apache.thrift.TException;

import tachyon.Constants;
import tachyon.MasterClientBase;
import tachyon.TachyonURI;
import tachyon.client.file.options.CompleteFileOptions;
import tachyon.client.file.options.CreateDirectoryOptions;
import tachyon.client.file.options.CreateFileOptions;
import tachyon.client.file.options.DeleteOptions;
import tachyon.client.file.options.FreeOptions;
import tachyon.client.file.options.LoadMetadataOptions;
import tachyon.client.file.options.SetAttributeOptions;
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
// TODO(calvin): Only use uris for rpcs.
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
   * Creates a new directory.
   *
   * @param path the directory path
   * @param options method options
   * @throws IOException if an I/O error occurs
   * @throws TachyonException if a Tachyon error occurs
   */
  public synchronized void createDirectory(final TachyonURI path,
      final CreateDirectoryOptions options) throws IOException, TachyonException {
    retryRPC(new RpcCallableThrowsTachyonTException<Boolean>() {
      @Override
      public Boolean call() throws TachyonTException, TException {
        return mClient.mkdir(path.getPath(), options.toThrift());
      }
    });
  }

  /**
   * Creates a new file.
   *
   * @param path the file path
   * @param options method options
   * @return the uri referencing the newly created file
   * @throws IOException if an I/O error occurs
   * @throws TachyonException if a Tachyon error occurs
   */
  public synchronized TachyonURI createFile(final TachyonURI path, final CreateFileOptions options)
      throws IOException, TachyonException {
    return retryRPC(new RpcCallableThrowsTachyonTException<TachyonURI>() {
      @Override
      public TachyonURI call() throws TachyonTException, TException {
        mClient.create(path.getPath(), options.toThrift());
        // TODO(calvin): Look into changing the master side implementation
        return path;
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
   * Marks a file as completed.
   *
   * @param path the file path
   * @param options the method options
   * @throws IOException if an I/O error occurs
   * @throws TachyonException if a Tachyon error occurs
   */
  public synchronized void completeFile(final TachyonURI path, final CompleteFileOptions options)
      throws IOException, TachyonException {
    retryRPC(new RpcCallableThrowsTachyonTException<Void>() {
      @Override
      public Void call() throws TachyonTException, TException {
        // TODO(calvin): Look into changing the master side implementation
        mClient.completeFile(mClient.getFileId(path.getPath()), options.toThrift());
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
   * Deletes a file or a directory.
   *
   * @param path the path to delete
   * @param options method options
   * @throws IOException if an I/O error occurs
   * @throws TachyonException if a Tachyon error occurs
   */
  public synchronized void delete(final TachyonURI path, final DeleteOptions options)
      throws IOException, TachyonException {
    retryRPC(new RpcCallableThrowsTachyonTException<Boolean>() {
      @Override
      public Boolean call() throws TachyonTException, TException {
        // TODO(calvin): Look into changing the master side implementation to take a uri
        return mClient.remove(mClient.getFileId(path.getPath()), options.isRecursive());
      }
    });
  }

  /**
   * @param path the path
   * @return the file id for the given path, or -1 if the path does not point to a file
   * @throws ConnectionFailedException if network connection failed
   * @throws IOException if an I/O error occurs
   */
  public synchronized long getFileId(final String path)
      throws IOException, ConnectionFailedException {
    return retryRPC(new RpcCallable<Long>() {
      @Override
      public Long call() throws TException {
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
   * @param path the file path
   * @return the file info for the given file id
   * @throws IOException if an I/O error occurs
   * @throws TachyonException if a Tachyon error occurs
   */
  public synchronized FileInfo getFileInfo(final TachyonURI path) throws IOException,
      TachyonException {
    return retryRPC(new RpcCallableThrowsTachyonTException<FileInfo>() {
      @Override
      public FileInfo call() throws TachyonTException, TException {
        // TODO(calvin): Look into changing the master side implementation to take a uri
        return mClient.getFileInfo(mClient.getFileId(path.getPath()));
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
   * @param path the path to list
   * @return the list of file information for the given path
   * @throws IOException if an I/O error occurs
   * @throws TachyonException if a Tachyon error occurs
   */
  public synchronized List<FileInfo> getFileInfoList(final TachyonURI path)
      throws IOException, TachyonException {
    return retryRPC(new RpcCallableThrowsTachyonTException<List<FileInfo>>() {
      @Override
      public List<FileInfo> call() throws TachyonTException, TException {
        return mClient.getFileInfoList(mClient.getFileId(path.getPath()));
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
   * @param path the file path
   * @return the next blockId for the file
   * @throws IOException if an I/O error occurs
   * @throws TachyonException if a Tachyon error occurs
   */
  public synchronized long getNewBlockIdForFile(final TachyonURI path)
      throws IOException, TachyonException {
    return retryRPC(new RpcCallableThrowsTachyonTException<Long>() {
      @Override
      public Long call() throws TachyonTException, TException {
        // TODO(calvin): Look into changing the master side implementation to take a uri
        return mClient.getNewBlockIdForFile(mClient.getFileId(path.getPath()));
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
   * Renames a file or a directory.
   *
   * @param id the id of the file to rename
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
   * Renames a file or a directory.
   *
   * @param src the path to rename
   * @param dst new file path
   * @return whether operation succeeded or not
   * @throws IOException if an I/O error occurs
   * @throws TachyonException if a Tachyon error occurs
   */
  public synchronized boolean rename(final TachyonURI src, final TachyonURI dst)
      throws IOException, TachyonException {
    return retryRPC(new RpcCallableThrowsTachyonTException<Boolean>() {
      @Override
      public Boolean call() throws TachyonTException, TException {
        // TODO(calvin): Look into changing the master side implementation to take a uri
        return mClient.rename(mClient.getFileId(src.getPath()), dst.getPath());
      }
    });
  }

  /**
   * Sets the file state.
   *
   * @param path the file path
   * @param options the file state options to be set
   * @throws IOException if an I/O error occurs
   * @throws TachyonException if a Tachyon error occurs
   */
  public synchronized void setAttribute(final TachyonURI path, final SetAttributeOptions options)
      throws IOException, TachyonException {
    retryRPC(new RpcCallableThrowsTachyonTException<Void>() {
      @Override
      public Void call() throws TachyonTException, TException {
        // TODO(calvin): Look into changing the master side implementation to take a uri
        mClient.setState(mClient.getFileId(path.getPath()), options.toThrift());
        return null;
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
   * Frees a file.
   *
   * @param path the path to free
   * @param options method options
   * @throws IOException if an I/O error occurs
   * @throws TachyonException if a Tachyon error occurs
   */
  public synchronized void free(final TachyonURI path, final FreeOptions options)
      throws IOException, TachyonException {
    retryRPC(new RpcCallableThrowsTachyonTException<Boolean>() {
      @Override
      public Boolean call() throws TachyonTException, TException {
        // TODO(calvin): Look into changing the master side implementation to take a uri
        return mClient.free(mClient.getFileId(path.getPath()), options.isRecursive());
      }
    });
  }

  /**
   * Loads a file from the under file system.
   *
   * @param path the Tachyon path of the file
   * @param recursive whether parent directories should be loaded if not present yet
   * @return the file id
   * @throws TachyonException if a Tachyon error occurs
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
   * Loads the metadata of a file from the under file system.
   *
   * @param path the path of the file to load metadata for
   * @param options method options
   * @throws TachyonException if a Tachyon error occurs
   * @throws IOException if an I/O error occurs
   */
  public synchronized void loadMetadata(final TachyonURI path,
      final LoadMetadataOptions options) throws IOException, TachyonException {
    retryRPC(new RpcCallableThrowsTachyonTException<Long>() {
      @Override
      public Long call() throws TachyonTException, TException {
        return mClient.loadMetadata(path.toString(), options.isRecursive());
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
