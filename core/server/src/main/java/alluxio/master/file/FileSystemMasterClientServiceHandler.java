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

package alluxio.master.file;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.RpcUtils;
import alluxio.RpcUtils.RpcCallable;
import alluxio.RpcUtils.RpcCallableThrowsIOException;
import alluxio.exception.AlluxioException;
import alluxio.master.file.options.CompleteFileOptions;
import alluxio.master.file.options.CreateDirectoryOptions;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.master.file.options.LoadMetadataOptions;
import alluxio.master.file.options.MountOptions;
import alluxio.master.file.options.SetAttributeOptions;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.CompleteFileTOptions;
import alluxio.thrift.CreateDirectoryTOptions;
import alluxio.thrift.CreateFileTOptions;
import alluxio.thrift.FileBlockInfo;
import alluxio.thrift.FileInfo;
import alluxio.thrift.FileSystemMasterClientService;
import alluxio.thrift.ListStatusTOptions;
import alluxio.thrift.MountTOptions;
import alluxio.thrift.SetAttributeTOptions;
import alluxio.thrift.ThriftIOException;
import alluxio.wire.ThriftUtils;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class is a Thrift handler for file system master RPCs invoked by an Alluxio client.
 */
@NotThreadSafe // TODO(jiri): make thread-safe (c.f. ALLUXIO-1664)
public final class FileSystemMasterClientServiceHandler implements
    FileSystemMasterClientService.Iface {
  private final FileSystemMaster mFileSystemMaster;

  /**
   * Creates a new instance of {@link FileSystemMasterClientServiceHandler}.
   *
   * @param fileSystemMaster the {@link FileSystemMaster} the handler uses internally
   */
  public FileSystemMasterClientServiceHandler(FileSystemMaster fileSystemMaster) {
    Preconditions.checkNotNull(fileSystemMaster);
    mFileSystemMaster = fileSystemMaster;
  }

  @Override
  public long getServiceVersion() {
    return Constants.FILE_SYSTEM_MASTER_CLIENT_SERVICE_VERSION;
  }

  @Override
  public void completeFile(final String path, final CompleteFileTOptions options)
      throws AlluxioTException {
    RpcUtils.call(new RpcCallable<Void>() {
      @Override
      public Void call() throws AlluxioException {
        mFileSystemMaster.completeFile(new AlluxioURI(path), new CompleteFileOptions(options));
        return null;
      }
    });
  }

  @Override
  public void createDirectory(final String path, final CreateDirectoryTOptions options)
      throws AlluxioTException, ThriftIOException {
    RpcUtils.call(new RpcCallableThrowsIOException<Void>() {
      @Override
      public Void call() throws AlluxioException, IOException {
        mFileSystemMaster.createDirectory(new AlluxioURI(path),
            new CreateDirectoryOptions(options));
        return null;
      }
    });
  }

  @Override
  public void createFile(final String path, final CreateFileTOptions options)
      throws AlluxioTException, ThriftIOException {
    RpcUtils.call(new RpcCallableThrowsIOException<Void>() {
      @Override
      public Void call() throws AlluxioException, IOException {
        mFileSystemMaster.createFile(new AlluxioURI(path), new CreateFileOptions(options));
        return null;
      }
    });
  }

  @Override
  public void free(final String path, final boolean recursive) throws AlluxioTException {
    RpcUtils.call(new RpcCallable<Void>() {
      @Override
      public Void call() throws AlluxioException {
        mFileSystemMaster.free(new AlluxioURI(path), recursive);
        return null;
      }
    });
  }

  /**
   * {@inheritDoc}
   *
   * @deprecated since version 1.1 and will be removed in version 2.0
   * @see {@link #getStatus(String)}
   */
  @Override
  @Deprecated
  public List<FileBlockInfo> getFileBlockInfoList(final String path) throws AlluxioTException {
    return RpcUtils.call(new RpcCallable<List<FileBlockInfo>>() {
      @Override
      public List<FileBlockInfo> call() throws AlluxioException {
        List<FileBlockInfo> result = new ArrayList<FileBlockInfo>();
        for (alluxio.wire.FileBlockInfo fileBlockInfo :
            mFileSystemMaster.getFileBlockInfoList(new AlluxioURI(path))) {
          result.add(ThriftUtils.toThrift(fileBlockInfo));
        }
        return result;
      }
    });
  }

  @Override
  public long getNewBlockIdForFile(final String path) throws AlluxioTException {
    return RpcUtils.call(new RpcCallable<Long>() {
      @Override
      public Long call() throws AlluxioException {
        return mFileSystemMaster.getNewBlockIdForFile(new AlluxioURI(path));
      }
    });
  }

  @Override
  public FileInfo getStatus(final String path) throws AlluxioTException {
    return RpcUtils.call(new RpcCallable<FileInfo>() {
      @Override
      public FileInfo call() throws AlluxioException {
        return ThriftUtils.toThrift(mFileSystemMaster.getFileInfo(new AlluxioURI(path)));
      }
    });
  }

  /**
   * {@inheritDoc}
   *
   * @deprecated since version 1.1 and will be removed in version 2.0
   */
  @Override
  @Deprecated
  public FileInfo getStatusInternal(final long fileId) throws AlluxioTException {
    return RpcUtils.call(new RpcCallable<FileInfo>() {
      @Override
      public FileInfo call() throws AlluxioException {
        return ThriftUtils.toThrift(mFileSystemMaster.getFileInfo(fileId));
      }
    });
  }

  /**
   * {@inheritDoc}
   *
   * @deprecated since version 1.1 and will be removed in version 2.0
   */
  @Override
  @Deprecated
  public String getUfsAddress() {
    return mFileSystemMaster.getUfsAddress();
  }

  @Override
  public List<FileInfo> listStatus(final String path, final ListStatusTOptions options)
      throws AlluxioTException {
    return RpcUtils.call(new RpcCallable<List<FileInfo>>() {
      @Override
      public List<FileInfo> call() throws AlluxioException {
        List<FileInfo> result = new ArrayList<FileInfo>();
        for (alluxio.wire.FileInfo fileInfo : mFileSystemMaster
            .getFileInfoList(new AlluxioURI(path), options.isLoadDirectChildren())) {
          result.add(ThriftUtils.toThrift(fileInfo));
        }
        return result;
      }
    });
  }

  /**
   * {@inheritDoc}
   *
   * @deprecated since version 1.1 and will be removed in version 2.0
   */
  @Override
  @Deprecated
  public long loadMetadata(final String alluxioPath, final boolean recursive)
      throws AlluxioTException, ThriftIOException {
    return RpcUtils.call(new RpcCallableThrowsIOException<Long>() {
      @Override
      public Long call() throws AlluxioException, IOException {
        return mFileSystemMaster.loadMetadata(new AlluxioURI(alluxioPath),
            LoadMetadataOptions.defaults().setCreateAncestors(true).setLoadDirectChildren(true));
      }
    });
  }

  @Override
  public void mount(final String alluxioPath, final String ufsPath, final MountTOptions options)
      throws AlluxioTException, ThriftIOException {
    RpcUtils.call(new RpcCallableThrowsIOException<Void>() {
      @Override
      public Void call() throws AlluxioException, IOException {
        mFileSystemMaster.mount(new AlluxioURI(alluxioPath), new AlluxioURI(ufsPath),
            new MountOptions(options));
        return null;
      }
    });
  }

  @Override
  public void remove(final String path, final boolean recursive)
      throws AlluxioTException, ThriftIOException {
    RpcUtils.call(new RpcCallableThrowsIOException<Void>() {
      @Override
      public Void call() throws AlluxioException, IOException {
        mFileSystemMaster.delete(new AlluxioURI(path), recursive);
        return null;
      }
    });
  }

  @Override
  public void rename(final String srcPath, final String dstPath)
      throws AlluxioTException, ThriftIOException {
    RpcUtils.call(new RpcCallableThrowsIOException<Void>() {
      @Override
      public Void call() throws AlluxioException, IOException {
        mFileSystemMaster.rename(new AlluxioURI(srcPath), new AlluxioURI(dstPath));
        return null;
      }
    });
  }

  @Override
  public void scheduleAsyncPersist(final String path) throws AlluxioTException {
    RpcUtils.call(new RpcCallable<Void>() {
      @Override
      public Void call() throws AlluxioException {
        mFileSystemMaster.scheduleAsyncPersistence(new AlluxioURI(path));
        return null;
      }
    });
  }

  // TODO(calvin): Do not rely on client side options
  @Override
  public void setAttribute(final String path, final SetAttributeTOptions options)
      throws AlluxioTException {
    RpcUtils.call(new RpcCallable<Void>() {
      @Override
      public Void call() throws AlluxioException {
          mFileSystemMaster.setAttribute(new AlluxioURI(path), new SetAttributeOptions(options));
          return null;
      }
    });
  }

  @Override
  public void unmount(final String alluxioPath) throws AlluxioTException, ThriftIOException {
    RpcUtils.call(new RpcCallableThrowsIOException<Void>() {
      @Override
      public Void call() throws AlluxioException, IOException {
        mFileSystemMaster.unmount(new AlluxioURI(alluxioPath));
        return null;
      }
    });
  }
}
