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
import alluxio.client.file.options.GetStatusOptions;
import alluxio.client.file.options.ListStatusOptions;
import alluxio.client.file.options.LoadMetadataOptions;
import alluxio.client.file.options.MountOptions;
import alluxio.client.file.options.RenameOptions;
import alluxio.client.file.options.SetAttributeOptions;
import alluxio.client.file.options.UpdateUfsModeOptions;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.master.MasterClientConfig;
import alluxio.thrift.AlluxioService;
import alluxio.thrift.FileSystemMasterClientService;
import alluxio.thrift.GetMountTableTResponse;
import alluxio.thrift.GetNewBlockIdForFileTOptions;
import alluxio.thrift.LoadMetadataTOptions;
import alluxio.thrift.ScheduleAsyncPersistenceTOptions;
import alluxio.thrift.UnmountTOptions;
import alluxio.wire.FileInfo;
import alluxio.wire.MountPointInfo;

import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

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
   * @param conf master client configuration
   */
  public RetryHandlingFileSystemMasterClient(MasterClientConfig conf) {
    super(conf);
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
  protected void afterConnect() {
    mClient = new FileSystemMasterClientService.Client(mProtocol);
  }

  @Override
  public synchronized List<AlluxioURI> checkConsistency(final AlluxioURI path,
      final CheckConsistencyOptions options) throws AlluxioStatusException {
    return retryRPC(new RpcCallable<List<AlluxioURI>>() {
      @Override
      public List<AlluxioURI> call() throws TException {
        List<String> inconsistentPaths =
            mClient.checkConsistency(path.getPath(), options.toThrift()).getInconsistentPaths();
        List<AlluxioURI> inconsistentUris = new ArrayList<>(inconsistentPaths.size());
        for (String inconsistentPath : inconsistentPaths) {
          inconsistentUris.add(new AlluxioURI(inconsistentPath));
        }
        return inconsistentUris;
      }

      @Override
      public String getName() {
        return "CheckConsistency";
      }
    });
  }

  @Override
  public synchronized void createDirectory(final AlluxioURI path,
      final CreateDirectoryOptions options) throws AlluxioStatusException {
    retryRPC(new RpcCallable<Void>() {
      @Override
      public Void call() throws TException {
        mClient.createDirectory(path.getPath(), options.toThrift());
        return null;
      }

      @Override
      public String getName() {
        return "CreateDirectory";
      }
    });
  }

  @Override
  public synchronized void createFile(final AlluxioURI path, final CreateFileOptions options)
      throws AlluxioStatusException {
    retryRPC(new RpcCallable<Void>() {
      @Override
      public Void call() throws TException {
        mClient.createFile(path.getPath(), options.toThrift());
        return null;
      }

      @Override
      public String getName() {
        return "CreateFile";
      }
    });
  }

  @Override
  public synchronized void completeFile(final AlluxioURI path, final CompleteFileOptions options)
      throws AlluxioStatusException {
    retryRPC(new RpcCallable<Object>() {
      @Override
      public Void call() throws TException {
        mClient.completeFile(path.getPath(), options.toThrift());
        return null;
      }

      @Override
      public String getName() {
        return "CompleteFile";
      }
    });
  }

  @Override
  public synchronized void delete(final AlluxioURI path, final DeleteOptions options)
      throws AlluxioStatusException {
    retryRPC(new RpcCallable<Void>() {
      @Override
      public Void call() throws TException {
        mClient.remove(path.getPath(), options.isRecursive(), options.toThrift());
        return null;
      }

      @Override
      public String getName() {
        return "Delete";
      }
    });
  }

  @Override
  public synchronized void free(final AlluxioURI path, final FreeOptions options)
      throws AlluxioStatusException {
    retryRPC(new RpcCallable<Void>() {
      @Override
      public Void call() throws TException {
        mClient.free(path.getPath(), options.isRecursive(), options.toThrift());
        return null;
      }

      @Override
      public String getName() {
        return "Free";
      }
    });
  }

  @Override
  public synchronized URIStatus getStatus(final AlluxioURI path, final GetStatusOptions options)
      throws AlluxioStatusException {
    return retryRPC(new RpcCallable<URIStatus>() {
      @Override
      public URIStatus call() throws TException {
        return new URIStatus(FileInfo.fromThrift(
            mClient.getStatus(path.getPath(), options.toThrift()).getFileInfo()));
      }

      @Override
      public String getName() {
        return "GetStatus";
      }
    });
  }

  @Override
  public synchronized long getNewBlockIdForFile(final AlluxioURI path)
      throws AlluxioStatusException {
    return retryRPC(new RpcCallable<Long>() {
      @Override
      public Long call() throws TException {
        return mClient.getNewBlockIdForFile(
            path.getPath(), new GetNewBlockIdForFileTOptions()).getId();
      }

      @Override
      public String getName() {
        return "GetNewBlockIdForFile";
      }
    });
  }

  @Override
  public synchronized Map<String, alluxio.wire.MountPointInfo> getMountTable()
      throws AlluxioStatusException {
    return retryRPC(new RpcCallable<Map<String, MountPointInfo>>() {
      @Override
      public Map<String, MountPointInfo> call() throws TException {
        GetMountTableTResponse result = mClient.getMountTable();
        Map<String, alluxio.thrift.MountPointInfo> mountTableThrift = result.getMountTable();
        Map<String, alluxio.wire.MountPointInfo> mountTableWire = new HashMap<>();
        for (Map.Entry<String, alluxio.thrift.MountPointInfo> entry : mountTableThrift.entrySet()) {
          alluxio.thrift.MountPointInfo mMountPointInfoThrift = entry.getValue();
          alluxio.wire.MountPointInfo mMountPointInfoWire =
              MountPointInfo.fromThrift(mMountPointInfoThrift);
          mountTableWire.put(entry.getKey(), mMountPointInfoWire);
        }
        return mountTableWire;
      }

      @Override
      public String getName() {
        return "GetMountTable";
      }
    });
  }

  @Override
  public synchronized List<URIStatus> listStatus(final AlluxioURI path,
      final ListStatusOptions options) throws AlluxioStatusException {
    return retryRPC(new RpcCallable<List<URIStatus>>() {
      @Override
      public List<URIStatus> call() throws TException {
        List<URIStatus> result = new ArrayList<>();
        for (alluxio.thrift.FileInfo fileInfo : mClient.listStatus(path.getPath(),
            options.toThrift()).getFileInfoList()) {
          result.add(new URIStatus(FileInfo.fromThrift(fileInfo)));
        }
        return result;
      }

      @Override
      public String getName() {
        return "ListStatus";
      }
    });
  }

  @Override
  public synchronized void loadMetadata(final AlluxioURI path,
      final LoadMetadataOptions options) throws AlluxioStatusException {
    retryRPC(new RpcCallable<Void>() {
      @Override
      public Void call() throws TException {
        mClient.loadMetadata(path.toString(), options.isRecursive(), new LoadMetadataTOptions());
        return null;
      }

      @Override
      public String getName() {
        return "LoadMetadata";
      }
    });
  }

  @Override
  public synchronized void mount(final AlluxioURI alluxioPath, final AlluxioURI ufsPath,
      final MountOptions options) throws AlluxioStatusException {
    retryRPC(new RpcCallable<Void>() {
      @Override
      public Void call() throws TException {
        mClient.mount(alluxioPath.toString(), ufsPath.toString(), options.toThrift());
        return null;
      }

      @Override
      public String getName() {
        return "Mount";
      }
    });
  }

  @Override
  public synchronized void rename(final AlluxioURI src, final AlluxioURI dst)
      throws AlluxioStatusException {
    rename(src, dst, RenameOptions.defaults());
  }

  @Override
  public synchronized void rename(final AlluxioURI src, final AlluxioURI dst,
      final RenameOptions options) throws AlluxioStatusException {
    retryRPC(new RpcCallable<Void>() {
      @Override
      public Void call() throws TException {
        mClient.rename(src.getPath(), dst.getPath(), options.toThrift());
        return null;
      }

      @Override
      public String getName() {
        return "Rename";
      }
    });
  }

  @Override
  public synchronized void setAttribute(final AlluxioURI path, final SetAttributeOptions options)
      throws AlluxioStatusException {
    retryRPC(new RpcCallable<Void>() {
      @Override
      public Void call() throws TException {
        mClient.setAttribute(path.getPath(), options.toThrift());
        return null;
      }

      @Override
      public String getName() {
        return "SetAttribute";
      }
    });
  }

  @Override
  public synchronized void scheduleAsyncPersist(final AlluxioURI path)
      throws AlluxioStatusException {
    retryRPC(new RpcCallable<Void>() {
      @Override
      public Void call() throws TException {
        mClient.scheduleAsyncPersistence(path.getPath(), new ScheduleAsyncPersistenceTOptions());
        return null;
      }

      @Override
      public String getName() {
        return "ScheduleAsyncPersist";
      }
    });
  }

  @Override
  public synchronized void unmount(final AlluxioURI alluxioPath) throws AlluxioStatusException {
    retryRPC(new RpcCallable<Void>() {
      @Override
      public Void call() throws TException {
        mClient.unmount(alluxioPath.toString(), new UnmountTOptions());
        return null;
      }

      @Override
      public String getName() {
        return "Unmount";
      }
    });
  }

  @Override
  public synchronized void updateUfsMode(final AlluxioURI ufsUri,
      final UpdateUfsModeOptions options) throws AlluxioStatusException {
    retryRPC(new RpcCallable<Void>() {
      @Override
      public Void call() throws TException {
        mClient.updateUfsMode(ufsUri.getRootPath(), options.toThrift());
        return null;
      }

      @Override
      public String getName() {
        return "UpdateUfsMode";
      }
    });
  }
}
