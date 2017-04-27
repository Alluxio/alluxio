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
import alluxio.master.file.options.CheckConsistencyOptions;
import alluxio.master.file.options.CompleteFileOptions;
import alluxio.master.file.options.CreateDirectoryOptions;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.master.file.options.DeleteOptions;
import alluxio.master.file.options.FreeOptions;
import alluxio.master.file.options.ListStatusOptions;
import alluxio.master.file.options.LoadMetadataOptions;
import alluxio.master.file.options.MountOptions;
import alluxio.master.file.options.RenameOptions;
import alluxio.master.file.options.SetAttributeOptions;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.CheckConsistencyTOptions;
import alluxio.thrift.CompleteFileTOptions;
import alluxio.thrift.CreateDirectoryTOptions;
import alluxio.thrift.CreateFileTOptions;
import alluxio.thrift.DeleteTOptions;
import alluxio.thrift.FileBlockInfo;
import alluxio.thrift.FileInfo;
import alluxio.thrift.FileSystemMasterClientService;
import alluxio.thrift.FreeTOptions;
import alluxio.thrift.ListStatusTOptions;
import alluxio.thrift.MountTOptions;
import alluxio.thrift.SetAttributeTOptions;
import alluxio.wire.ThriftUtils;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final Logger LOG =
      LoggerFactory.getLogger(FileSystemMasterClientServiceHandler.class);
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
  public List<String> checkConsistency(final String path, final CheckConsistencyTOptions options)
      throws AlluxioTException {
    return RpcUtils.callAndLog(LOG, new RpcCallableThrowsIOException<List<String>>() {
      @Override
      public List<String> call() throws AlluxioException, IOException {
        List<AlluxioURI> inconsistentUris = mFileSystemMaster.checkConsistency(
            new AlluxioURI(path), new CheckConsistencyOptions(options));
        List<String> uris = new ArrayList<>(inconsistentUris.size());
        for (AlluxioURI uri : inconsistentUris) {
          uris.add(uri.getPath());
        }
        return uris;
      }

      @Override
      public String toString() {
        return String.format("CheckConsistency: path=%s, options=%s", path, options);
      }
    });
  }

  @Override
  public void completeFile(final String path, final CompleteFileTOptions options)
      throws AlluxioTException {
    RpcUtils.callAndLog(LOG, new RpcCallable<Void>() {
      @Override
      public Void call() throws AlluxioException {
        mFileSystemMaster.completeFile(new AlluxioURI(path), new CompleteFileOptions(options));
        return null;
      }

      @Override
      public String toString() {
        return String.format("CompleteFile: path=%s, options=%s", path, options);
      }
    });
  }

  @Override
  public void createDirectory(final String path, final CreateDirectoryTOptions options)
      throws AlluxioTException {
    RpcUtils.callAndLog(LOG, new RpcCallableThrowsIOException<Void>() {
      @Override
      public Void call() throws AlluxioException, IOException {
        mFileSystemMaster.createDirectory(new AlluxioURI(path),
            new CreateDirectoryOptions(options));
        return null;
      }

      @Override
      public String toString() {
        return String.format("CreateDirectory: path=%s, options=%s", path, options);
      }
    });
  }

  @Override
  public void createFile(final String path, final CreateFileTOptions options)
      throws AlluxioTException {
    RpcUtils.callAndLog(LOG, new RpcCallableThrowsIOException<Void>() {
      @Override
      public Void call() throws AlluxioException, IOException {
        mFileSystemMaster.createFile(new AlluxioURI(path), new CreateFileOptions(options));
        return null;
      }

      @Override
      public String toString() {
        return String.format("CreateFile: path=%s, options=%s", path, options);
      }
    });
  }

  @Override
  public void free(final String path, final boolean recursive, final FreeTOptions options)
      throws AlluxioTException {
    RpcUtils.callAndLog(LOG, new RpcCallable<Void>() {
      @Override
      public Void call() throws AlluxioException {
        if (options == null) {
          // For Alluxio client v1.4 or earlier.
          // NOTE, we try to be conservative here so early Alluxio clients will not be able to force
          // freeing pinned items but see the error thrown.
          mFileSystemMaster.free(new AlluxioURI(path),
              FreeOptions.defaults().setRecursive(recursive));
        } else {
          mFileSystemMaster.free(new AlluxioURI(path), new FreeOptions(options));
        }
        return null;
      }

      @Override
      public String toString() {
        return String.format("Free: path=%s, recursive=%s, options=%s", path, recursive, options);
      }
    });
  }

  /**
   * {@inheritDoc}
   *
   * @deprecated since version 1.1 and will be removed in version 2.0
   * @see #getStatus(String)
   */
  @Override
  @Deprecated
  public List<FileBlockInfo> getFileBlockInfoList(final String path) throws AlluxioTException {
    return RpcUtils.callAndLog(LOG, new RpcCallable<List<FileBlockInfo>>() {
      @Override
      public List<FileBlockInfo> call() throws AlluxioException {
        List<FileBlockInfo> result = new ArrayList<>();
        for (alluxio.wire.FileBlockInfo fileBlockInfo :
            mFileSystemMaster.getFileBlockInfoList(new AlluxioURI(path))) {
          result.add(ThriftUtils.toThrift(fileBlockInfo));
        }
        return result;
      }

      @Override
      public String toString() {
        return String.format("GetFileBlockInfoList: path=%s", path);
      }
    });
  }

  @Override
  public long getNewBlockIdForFile(final String path) throws AlluxioTException {
    return RpcUtils.callAndLog(LOG, new RpcCallable<Long>() {
      @Override
      public Long call() throws AlluxioException {
        return mFileSystemMaster.getNewBlockIdForFile(new AlluxioURI(path));
      }

      @Override
      public String toString() {
        return String.format("GetNewBlockIdForFile: path=%s", path);
      }
    });
  }

  @Override
  public FileInfo getStatus(final String path) throws AlluxioTException {
    return RpcUtils.callAndLog(LOG, new RpcCallable<FileInfo>() {
      @Override
      public FileInfo call() throws AlluxioException {
        return ThriftUtils.toThrift(mFileSystemMaster.getFileInfo(new AlluxioURI(path)));
      }

      @Override
      public String toString() {
        return String.format("GetStatus: path=%s", path);
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
    return RpcUtils.callAndLog(LOG, new RpcCallable<FileInfo>() {
      @Override
      public FileInfo call() throws AlluxioException {
        return ThriftUtils.toThrift(mFileSystemMaster.getFileInfo(fileId));
      }

      @Override
      public String toString() {
        return String.format("GetStatusInternal: fileId=%s", fileId);
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
  public String getUfsAddress() throws AlluxioTException {
    return RpcUtils.callAndLog(LOG, new RpcCallable<String>() {
      @Override
      public String call() throws AlluxioException {
        return mFileSystemMaster.getUfsAddress();
      }

      @Override
      public String toString() {
        return String.format("GetUfsAddress");
      }
    });
  }

  @Override
  public List<FileInfo> listStatus(final String path, final ListStatusTOptions options)
      throws AlluxioTException {
    return RpcUtils.callAndLog(LOG, new RpcCallable<List<FileInfo>>() {
      @Override
      public List<FileInfo> call() throws AlluxioException {
        List<FileInfo> result = new ArrayList<>();
        for (alluxio.wire.FileInfo fileInfo : mFileSystemMaster
            .listStatus(new AlluxioURI(path), new ListStatusOptions(options))) {
          result.add(ThriftUtils.toThrift(fileInfo));
        }
        return result;
      }

      @Override
      public String toString() {
        return String.format("ListStatus: path=%s, options=%s", path, options);
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
      throws AlluxioTException {
    return RpcUtils.callAndLog(LOG, new RpcCallableThrowsIOException<Long>() {
      @Override
      public Long call() throws AlluxioException, IOException {
        return mFileSystemMaster.loadMetadata(new AlluxioURI(alluxioPath),
            LoadMetadataOptions.defaults().setCreateAncestors(true).setLoadDirectChildren(true));
      }

      @Override
      public String toString() {
        return String.format("LoadMetadata: alluxioPath=%s, recursive=%s", alluxioPath, recursive);
      }
    });
  }

  @Override
  public void mount(final String alluxioPath, final String ufsPath, final MountTOptions options)
      throws AlluxioTException {
    RpcUtils.callAndLog(LOG, new RpcCallableThrowsIOException<Void>() {
      @Override
      public Void call() throws AlluxioException, IOException {
        mFileSystemMaster.mount(new AlluxioURI(alluxioPath), new AlluxioURI(ufsPath),
            new MountOptions(options));
        return null;
      }

      @Override
      public String toString() {
        return String.format("Mount: alluxioPath=%s, ufsPath=%s, options=%s", alluxioPath, ufsPath,
            options);
      }
    });
  }

  @Override
  public void remove(final String path, final boolean recursive, final DeleteTOptions options)
      throws AlluxioTException {
    RpcUtils.callAndLog(LOG, new RpcCallableThrowsIOException<Void>() {
      @Override
      public Void call() throws AlluxioException, IOException {
        if (options == null) {
          // For Alluxio client v1.4 or earlier.
          // NOTE, we try to be conservative here so early Alluxio clients will not be able to
          // delete files in Alluxio only.
          mFileSystemMaster.delete(new AlluxioURI(path),
              DeleteOptions.defaults().setRecursive(recursive));
        } else {
          mFileSystemMaster.delete(new AlluxioURI(path), new DeleteOptions(options));
        }
        return null;
      }

      @Override
      public String toString() {
        return String.format("Remove: path=%s, recursive=%s", path, recursive);
      }
    });
  }

  @Override
  public void rename(final String srcPath, final String dstPath)
      throws AlluxioTException {
    RpcUtils.callAndLog(LOG, new RpcCallableThrowsIOException<Void>() {
      @Override
      public Void call() throws AlluxioException, IOException {
        mFileSystemMaster
            .rename(new AlluxioURI(srcPath), new AlluxioURI(dstPath), RenameOptions.defaults());
        return null;
      }

      @Override
      public String toString() {
        return String.format("Rename: srcPath=%s, dstPath=%s", srcPath, dstPath);
      }
    });
  }

  @Override
  public void scheduleAsyncPersist(final String path) throws AlluxioTException {
    RpcUtils.callAndLog(LOG, new RpcCallable<Void>() {
      @Override
      public Void call() throws AlluxioException {
        mFileSystemMaster.scheduleAsyncPersistence(new AlluxioURI(path));
        return null;
      }

      @Override
      public String toString() {
        return String.format("ScheduleAsyncPersist: path=%s", path);
      }
    });
  }

  // TODO(calvin): Do not rely on client side options
  @Override
  public void setAttribute(final String path, final SetAttributeTOptions options)
      throws AlluxioTException {
    RpcUtils.callAndLog(LOG, new RpcCallable<Void>() {
      @Override
      public Void call() throws AlluxioException {
        mFileSystemMaster.setAttribute(new AlluxioURI(path), new SetAttributeOptions(options));
        return null;
      }

      @Override
      public String toString() {
        return String.format("SetAttribute: path=%s, options=%s", path, options);
      }
    });
  }

  @Override
  public void unmount(final String alluxioPath) throws AlluxioTException {
    RpcUtils.callAndLog(LOG, new RpcCallableThrowsIOException<Void>() {
      @Override
      public Void call() throws AlluxioException, IOException {
        mFileSystemMaster.unmount(new AlluxioURI(alluxioPath));
        return null;
      }

      @Override
      public String toString() {
        return String.format("Unmount: alluxioPath=%s", alluxioPath);
      }
    });
  }
}
