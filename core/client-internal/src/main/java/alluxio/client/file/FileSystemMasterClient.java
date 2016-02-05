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

package alluxio.client.file;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.thrift.TException;

import alluxio.Constants;
import alluxio.MasterClientBase;
import alluxio.AlluxioURI;
import alluxio.client.file.options.CompleteFileOptions;
import alluxio.client.file.options.CreateDirectoryOptions;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.DeleteOptions;
import alluxio.client.file.options.FreeOptions;
import alluxio.client.file.options.LoadMetadataOptions;
import alluxio.client.file.options.SetAttributeOptions;
import alluxio.Configuration;
import alluxio.exception.ConnectionFailedException;
import alluxio.exception.AlluxioException;
import alluxio.thrift.FileSystemMasterClientService;
import alluxio.thrift.AlluxioService;
import alluxio.thrift.AlluxioTException;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.ThriftUtils;

/**
 * A wrapper for the thrift client to interact with the file system master, used by alluxio clients.
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
   * @param configuration the Tachyon configuration
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
   * @throws AlluxioException if a Tachyon error occurs
   */
  public synchronized void createDirectory(final AlluxioURI path,
      final CreateDirectoryOptions options) throws IOException, AlluxioException {
    retryRPC(new RpcCallableThrowsAlluxioTException<Void>() {
      @Override
      public Void call() throws AlluxioTException, TException {
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
   * @throws AlluxioException if a Tachyon error occurs
   */
  public synchronized void createFile(final AlluxioURI path, final CreateFileOptions options)
      throws IOException, AlluxioException {
    retryRPC(new RpcCallableThrowsAlluxioTException<Void>() {
      @Override
      public Void call() throws AlluxioTException, TException {
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
   * @throws AlluxioException if a Tachyon error occurs
   */
  public synchronized void completeFile(final AlluxioURI path, final CompleteFileOptions options)
      throws IOException, AlluxioException {
    retryRPC(new RpcCallableThrowsAlluxioTException<Void>() {
      @Override
      public Void call() throws AlluxioTException, TException {
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
   * @throws AlluxioException if a Tachyon error occurs
   */
  public synchronized void delete(final AlluxioURI path, final DeleteOptions options)
      throws IOException, AlluxioException {
    retryRPC(new RpcCallableThrowsAlluxioTException<Void>() {
      @Override
      public Void call() throws AlluxioTException, TException {
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
   * @throws AlluxioException if a Tachyon error occurs
   */
  public synchronized void free(final AlluxioURI path, final FreeOptions options)
      throws IOException, AlluxioException {
    retryRPC(new RpcCallableThrowsAlluxioTException<Void>() {
      @Override
      public Void call() throws AlluxioTException, TException {
        mClient.free(path.getPath(), options.isRecursive());
        return null;
      }
    });
  }

  /**
   * @param path the URI of the file
   * @return the list of file block information for the given file id
   * @throws IOException if an I/O error occurs
   * @throws AlluxioException if a Tachyon error occurs
   */
  public synchronized List<FileBlockInfo> getFileBlockInfoList(final AlluxioURI path)
      throws IOException, AlluxioException {
    return retryRPC(new RpcCallableThrowsAlluxioTException<List<FileBlockInfo>>() {
      @Override
      public List<FileBlockInfo> call() throws AlluxioTException, TException {
        List<FileBlockInfo> result = new ArrayList<FileBlockInfo>();
        for (alluxio.thrift.FileBlockInfo fileBlockInfo :
            mClient.getFileBlockInfoList(path.getPath())) {
          result.add(ThriftUtils.fromThrift(fileBlockInfo));
        }
        return result;
      }
    });
  }

  /**
   * @param path the file path
   * @return the file info for the given file id
   * @throws IOException if an I/O error occurs
   * @throws AlluxioException if a Tachyon error occurs
   */
  public synchronized URIStatus getStatus(final AlluxioURI path) throws IOException,
      AlluxioException {
    return retryRPC(new RpcCallableThrowsAlluxioTException<URIStatus>() {
      @Override
      public URIStatus call() throws AlluxioTException, TException {
        return new URIStatus(ThriftUtils.fromThrift(mClient.getStatus(path.getPath())));
      }
    });
  }

  /**
   * Internal API, only used by the WebUI of the servers.
   *
   * @param fileId the file id
   * @return the file info for the given file id
   * @throws IOException if an I/O error occurs
   * @throws AlluxioException if a Tachyon error occurs
   */
  // TODO(calvin): Split this into its own client
  public synchronized URIStatus getStatusInternal(final long fileId) throws IOException,
      AlluxioException {
    return retryRPC(new RpcCallableThrowsAlluxioTException<URIStatus>() {
      @Override
      public URIStatus call() throws AlluxioTException, TException {
        return new URIStatus(ThriftUtils.fromThrift(mClient.getStatusInternal(fileId)));
      }
    });
  }

  /**
   * @param path the file path
   * @return the next blockId for the file
   * @throws IOException if an I/O error occurs
   * @throws AlluxioException if a Tachyon error occurs
   */
  public synchronized long getNewBlockIdForFile(final AlluxioURI path)
      throws IOException, AlluxioException {
    return retryRPC(new RpcCallableThrowsAlluxioTException<Long>() {
      @Override
      public Long call() throws AlluxioTException, TException {
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
   * @throws AlluxioException if a Tachyon error occurs
   */
  public synchronized List<URIStatus> listStatus(final AlluxioURI path)
      throws IOException, AlluxioException {
    return retryRPC(new RpcCallableThrowsAlluxioTException<List<URIStatus>>() {
      @Override
      public List<URIStatus> call() throws AlluxioTException, TException {
        List<URIStatus> result = new ArrayList<URIStatus>();
        for (alluxio.thrift.FileInfo fileInfo : mClient.listStatus(path.getPath())) {
          result.add(new URIStatus(ThriftUtils.fromThrift(fileInfo)));
        }
        return result;
      }
    });
  }

  /**
   * Loads the metadata of a file from the under file system.
   *
   * @param path the path of the file to load metadata for
   * @param options method options
   * @throws AlluxioException if a Tachyon error occurs
   * @throws IOException if an I/O error occurs
   */
  public synchronized void loadMetadata(final AlluxioURI path,
      final LoadMetadataOptions options) throws IOException, AlluxioException {
    retryRPC(new RpcCallableThrowsAlluxioTException<Long>() {
      @Override
      public Long call() throws AlluxioTException, TException {
        return mClient.loadMetadata(path.toString(), options.isRecursive());
      }
    });
  }

  /**
   * Mounts the given UFS path under the given Tachyon path.
   *
   * @param tachyonPath the Tachyon path
   * @param ufsPath the UFS path
   * @throws AlluxioException if a Tachyon error occurs
   * @throws IOException an I/O error occurs
   */
  public synchronized void mount(final AlluxioURI tachyonPath, final AlluxioURI ufsPath)
      throws AlluxioException, IOException {
    retryRPC(new RpcCallableThrowsAlluxioTException<Void>() {
      @Override
      public Void call() throws AlluxioTException, TException {
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
   * @throws AlluxioException if a Tachyon error occurs
   */
  public synchronized void rename(final AlluxioURI src, final AlluxioURI dst)
      throws IOException, AlluxioException {
    retryRPC(new RpcCallableThrowsAlluxioTException<Void>() {
      @Override
      public Void call() throws AlluxioTException, TException {
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
   * @throws AlluxioException if a Tachyon error occurs
   */
  public synchronized void setAttribute(final AlluxioURI path, final SetAttributeOptions options)
      throws IOException, AlluxioException {
    retryRPC(new RpcCallableThrowsAlluxioTException<Void>() {
      @Override
      public Void call() throws AlluxioTException, TException {
        mClient.setAttribute(path.getPath(), options.toThrift());
        return null;
      }
    });
  }

  /**
   * Schedules the async persistence of the given file.
   *
   * @param path the file path
   * @throws AlluxioException if a Tachyon error occurs
   * @throws IOException if an I/O error occurs
   */
  public synchronized void scheduleAsyncPersist(final AlluxioURI path)
      throws AlluxioException, IOException {
    retryRPC(new RpcCallableThrowsAlluxioTException<Void>() {
      @Override
      public Void call() throws AlluxioTException, TException {
        mClient.scheduleAsyncPersist(path.getPath());
        return null;
      }
    });
  }

  /**
   * Unmounts the given Tachyon path.
   *
   * @param tachyonPath the Tachyon path
   * @throws AlluxioException if a Tachyon error occurs
   * @throws IOException an I/O error occurs
   */
  public synchronized void unmount(final AlluxioURI tachyonPath)
      throws AlluxioException, IOException {
    retryRPC(new RpcCallableThrowsAlluxioTException<Void>() {
      @Override
      public Void call() throws AlluxioTException, TException {
        mClient.unmount(tachyonPath.toString());
        return null;
      }
    });
  }
}
