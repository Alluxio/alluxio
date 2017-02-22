/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.file;

import alluxio.AbstractMasterClient;
import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.options.CheckConsistencyOptions;
import alluxio.client.file.options.CompleteFileOptions;
import alluxio.client.file.options.CreateDirectoryOptions;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.DeleteOptions;
import alluxio.client.file.options.FreeOptions;
import alluxio.client.file.options.ListStatusOptions;
import alluxio.client.file.options.LoadMetadataOptions;
import alluxio.client.file.options.MountOptions;
import alluxio.client.file.options.SetAttributeOptions;
import alluxio.exception.AlluxioException;
import alluxio.thrift.AlluxioService;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.FileSystemMasterClientService;
import alluxio.wire.ThriftUtils;

import org.apache.thrift.TException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;
import javax.security.auth.Subject;

/**
 * A wrapper for the thrift client to interact with the file system master, used by alluxio clients.
 *
 * Since thrift clients are not thread safe, this class is a wrapper to provide thread safety, and
 * to provide retries.
 */
@ThreadSafe
public final class RetryHandlingFileSystemMasterClient extends AbstractMasterClient
    implements FileSystemMasterClient {
  private FileSystemMasterClientService.Client mClient = null;

  /**
   * Creates a new {@link RetryHandlingFileSystemMasterClient} instance.
   *
   * @param subject the subject
   * @param masterAddress the master address
   */
  protected static RetryHandlingFileSystemMasterClient create(Subject subject,
      InetSocketAddress masterAddress) {
    return new RetryHandlingFileSystemMasterClient(subject, masterAddress);
  }

  private RetryHandlingFileSystemMasterClient(Subject subject, InetSocketAddress masterAddress) {
    super(subject, masterAddress);
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

  @Override
  public synchronized List<AlluxioURI> checkConsistency(final AlluxioURI path,
      final CheckConsistencyOptions options) throws AlluxioException, IOException {
    return retryRPC(new RpcCallableThrowsAlluxioTException<List<AlluxioURI>>() {
      @Override
      public List<AlluxioURI> call() throws AlluxioTException, TException {
        List<String> inconsistentPaths =
            mClient.checkConsistency(path.getPath(), options.toThrift());
        List<AlluxioURI> inconsistentUris = new ArrayList<>(inconsistentPaths.size());
        for (String path : inconsistentPaths) {
          inconsistentUris.add(new AlluxioURI(path));
        }
        return inconsistentUris;
      }
    });
  }

  @Override
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

  @Override
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

  @Override
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

  @Override
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

  @Override
  public synchronized void free(final AlluxioURI path, final FreeOptions options)
      throws IOException, AlluxioException {
    retryRPC(new RpcCallableThrowsAlluxioTException<Void>() {
      @Override
      public Void call() throws AlluxioTException, TException {
        mClient.free(path.getPath(), options.isRecursive(), options.toThrift());
        return null;
      }
    });
  }

  @Override
  public synchronized URIStatus getStatus(final AlluxioURI path) throws IOException,
      AlluxioException {
    return retryRPC(new RpcCallableThrowsAlluxioTException<URIStatus>() {
      @Override
      public URIStatus call() throws AlluxioTException, TException {
        return new URIStatus(ThriftUtils.fromThrift(mClient.getStatus(path.getPath())));
      }
    });
  }

  @Override
  public synchronized long getNewBlockIdForFile(final AlluxioURI path)
      throws IOException, AlluxioException {
    return retryRPC(new RpcCallableThrowsAlluxioTException<Long>() {
      @Override
      public Long call() throws AlluxioTException, TException {
        return mClient.getNewBlockIdForFile(path.getPath());
      }
    });
  }

  @Override
  public synchronized List<URIStatus> listStatus(final AlluxioURI path,
      final ListStatusOptions options) throws IOException, AlluxioException {
    return retryRPC(new RpcCallableThrowsAlluxioTException<List<URIStatus>>() {
      @Override
      public List<URIStatus> call() throws AlluxioTException, TException {
        List<URIStatus> result = new ArrayList<URIStatus>();
        for (alluxio.thrift.FileInfo fileInfo : mClient
            .listStatus(path.getPath(), options.toThrift())) {
          result.add(new URIStatus(ThriftUtils.fromThrift(fileInfo)));
        }
        return result;
      }
    });
  }

  @Override
  public synchronized void loadMetadata(final AlluxioURI path,
      final LoadMetadataOptions options) throws IOException, AlluxioException {
    retryRPC(new RpcCallableThrowsAlluxioTException<Long>() {
      @Override
      public Long call() throws AlluxioTException, TException {
        return mClient.loadMetadata(path.toString(), options.isRecursive());
      }
    });
  }

  @Override
  public synchronized void mount(final AlluxioURI alluxioPath, final AlluxioURI ufsPath,
      final MountOptions options)
      throws AlluxioException, IOException {
    retryRPC(new RpcCallableThrowsAlluxioTException<Void>() {
      @Override
      public Void call() throws AlluxioTException, TException {
        mClient.mount(alluxioPath.toString(), ufsPath.toString(), options.toThrift());
        return null;
      }
    });
  }

  @Override
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

  @Override
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

  @Override
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

  @Override
  public synchronized void unmount(final AlluxioURI alluxioPath)
      throws AlluxioException, IOException {
    retryRPC(new RpcCallableThrowsAlluxioTException<Void>() {
      @Override
      public Void call() throws AlluxioTException, TException {
        mClient.unmount(alluxioPath.toString());
        return null;
      }
    });
  }
}
