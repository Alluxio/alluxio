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
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

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
   * Creates a new directory.
   *
   * @param path the directory path
   * @param options method options
   * @throws IOException if an I/O error occurs
   * @throws TachyonException if a Tachyon error occurs
   */
  public synchronized void createDirectory(final TachyonURI path,
      final CreateDirectoryOptions options) throws IOException, TachyonException {
    retryRPC(new RpcCallableThrowsTachyonTException<Void>() {
      @Override
      public Void call() throws TachyonTException, TException {
        mClient.createDirectory(path.getPath(), options.toThrift());
        return null;
      }
    });
  }

  /**
   * Creates a new file.
   *
   * @param path the file path
   * @param options method options
   * @throws IOException if an I/O error occurs
   * @throws TachyonException if a Tachyon error occurs
   */
  public synchronized void createFile(final TachyonURI path, final CreateFileOptions options)
      throws IOException, TachyonException {
    retryRPC(new RpcCallableThrowsTachyonTException<Void>() {
      @Override
      public Void call() throws TachyonTException, TException {
        mClient.createFile(path.getPath(), options.toThrift());
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
        mClient.completeFile(path.getPath(), options.toThrift());
        return null;
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
    retryRPC(new RpcCallableThrowsTachyonTException<Void>() {
      @Override
      public Void call() throws TachyonTException, TException {
        mClient.remove(path.getPath(), options.isRecursive());
        return null;
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
    retryRPC(new RpcCallableThrowsTachyonTException<Void>() {
      @Override
      public Void call() throws TachyonTException, TException {
        mClient.free(path.getPath(), options.isRecursive());
        return null;
      }
    });
  }

  /**
   * @param path the URI of the file
   * @return the list of file block information for the given file id
   * @throws IOException if an I/O error occurs
   * @throws TachyonException if a Tachyon error occurs
   */
  public synchronized List<FileBlockInfo> getFileBlockInfoList(final TachyonURI path)
      throws IOException, TachyonException {
    return retryRPC(new RpcCallableThrowsTachyonTException<List<FileBlockInfo>>() {
      @Override
      public List<FileBlockInfo> call() throws TachyonTException, TException {
        return mClient.getFileBlockInfoList(path.getPath());
      }
    });
  }

  /**
   * @param path the file path
   * @return the file info for the given file id
   * @throws IOException if an I/O error occurs
   * @throws TachyonException if a Tachyon error occurs
   */
  public synchronized URIStatus getStatus(final TachyonURI path) throws IOException,
      TachyonException {
    return retryRPC(new RpcCallableThrowsTachyonTException<URIStatus>() {
      @Override
      public URIStatus call() throws TachyonTException, TException {
        return new URIStatus(mClient.getStatus(path.getPath()));
      }
    });
  }

  /**
   * Internal API, only used by the WebUI of the servers.
   *
   * @param fileId the file id
   * @return the file info for the given file id
   * @throws IOException if an I/O error occurs
   * @throws TachyonException if a Tachyon error occurs
   */
  // TODO(calvin): Split this into its own client
  public synchronized URIStatus getStatusInternal(final long fileId) throws IOException,
      TachyonException {
    return retryRPC(new RpcCallableThrowsTachyonTException<URIStatus>() {
      @Override
      public URIStatus call() throws TachyonTException, TException {
        return new URIStatus(mClient.getStatusInternal(fileId));
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
        return mClient.getNewBlockIdForFile(path.getPath());
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
   * @param path the path to list
   * @return the list of file information for the given path
   * @throws IOException if an I/O error occurs
   * @throws TachyonException if a Tachyon error occurs
   */
  public synchronized List<URIStatus> listStatus(final TachyonURI path)
      throws IOException, TachyonException {
    return retryRPC(new RpcCallableThrowsTachyonTException<List<URIStatus>>() {
      @Override
      public List<URIStatus> call() throws TachyonTException, TException {
        List<FileInfo> statuses = mClient.listStatus(path.getPath());
        List<URIStatus> ret = new ArrayList<URIStatus>(statuses.size());
        for (FileInfo status : statuses) {
          ret.add(new URIStatus(status));
        }
        return ret;
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
   * @throws TachyonException if a Tachyon error occurs
   * @throws IOException an I/O error occurs
   */
  public synchronized void mount(final TachyonURI tachyonPath, final TachyonURI ufsPath)
      throws TachyonException, IOException {
    retryRPC(new RpcCallableThrowsTachyonTException<Void>() {
      @Override
      public Void call() throws TachyonTException, TException {
        mClient.mount(tachyonPath.toString(), ufsPath.toString());
        return null;
      }
    });
  }

  /**
   * Renames a file or a directory.
   *
   * @param src the path to rename
   * @param dst new file path
   * @throws IOException if an I/O error occurs
   * @throws TachyonException if a Tachyon error occurs
   */
  public synchronized void rename(final TachyonURI src, final TachyonURI dst)
      throws IOException, TachyonException {
    retryRPC(new RpcCallableThrowsTachyonTException<Void>() {
      @Override
      public Void call() throws TachyonTException, TException {
        mClient.rename(src.getPath(), dst.getPath());
        return null;
      }
    });
  }

  /**
   * Sets the file or directory attributes.
   *
   * @param path the file or directory path
   * @param options the file or directory attribute options to be set
   * @throws IOException if an I/O error occurs
   * @throws TachyonException if a Tachyon error occurs
   */
  public synchronized void setAttribute(final TachyonURI path, final SetAttributeOptions options)
      throws IOException, TachyonException {
    retryRPC(new RpcCallableThrowsTachyonTException<Void>() {
      @Override
      public Void call() throws TachyonTException, TException {
        mClient.setAttribute(path.getPath(), options.toThrift());
        return null;
      }
    });
  }

  /**
   * Schedules the async persistence of the given file.
   *
   * @param path the file path
   * @throws TachyonException if a Tachyon error occurs
   * @throws IOException if an I/O error occurs
   */
  public synchronized void scheduleAsyncPersist(final TachyonURI path)
      throws TachyonException, IOException {
    retryRPC(new RpcCallableThrowsTachyonTException<Void>() {
      @Override
      public Void call() throws TachyonTException, TException {
        mClient.scheduleAsyncPersist(path.getPath());
        return null;
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
   * Unmounts the given Tachyon path.
   *
   * @param tachyonPath the Tachyon path
   * @throws TachyonException if a Tachyon error occurs
   * @throws IOException an I/O error occurs
   */
  public synchronized void unmount(final TachyonURI tachyonPath)
      throws TachyonException, IOException {
    retryRPC(new RpcCallableThrowsTachyonTException<Void>() {
      @Override
      public Void call() throws TachyonTException, TException {
        mClient.unmount(tachyonPath.toString());
        return null;
      }
    });
  }
}
