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

package alluxio.master.file;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.master.file.options.CompleteFileOptions;
import alluxio.master.file.options.CreateDirectoryOptions;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.master.file.options.SetAttributeOptions;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.CompleteFileTOptions;
import alluxio.thrift.CreateDirectoryTOptions;
import alluxio.thrift.CreateFileTOptions;
import alluxio.thrift.FileBlockInfo;
import alluxio.thrift.FileInfo;
import alluxio.thrift.FileSystemMasterClientService;
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
  public void completeFile(String path, CompleteFileTOptions options) throws AlluxioTException {
    try {
      mFileSystemMaster.completeFile(new AlluxioURI(path), new CompleteFileOptions(options));
    } catch (AlluxioException e) {
      throw e.toAlluxioTException();
    }
  }

  @Override
  public void createDirectory(String path, CreateDirectoryTOptions options)
      throws AlluxioTException, ThriftIOException {
    try {
      mFileSystemMaster.mkdir(new AlluxioURI(path), new CreateDirectoryOptions(options));
    } catch (AlluxioException e) {
      throw e.toAlluxioTException();
    } catch (IOException e) {
      throw new ThriftIOException(e.getMessage());
    }
  }

  @Override
  public void createFile(String path, CreateFileTOptions options) throws AlluxioTException,
      ThriftIOException {
    try {
      mFileSystemMaster.create(new AlluxioURI(path), new CreateFileOptions(options));
    } catch (IOException e) {
      throw new ThriftIOException(e.getMessage());
    } catch (AlluxioException e) {
      throw e.toAlluxioTException();
    }
  }

  @Override
  public void free(String path, boolean recursive) throws AlluxioTException {
    try {
      mFileSystemMaster.free(new AlluxioURI(path), recursive);
    } catch (AlluxioException e) {
      throw e.toAlluxioTException();
    }
  }

  @Override
  public List<FileBlockInfo> getFileBlockInfoList(String path) throws AlluxioTException {
    try {
      List<FileBlockInfo> result = new ArrayList<FileBlockInfo>();
      for (alluxio.wire.FileBlockInfo fileBlockInfo :
          mFileSystemMaster.getFileBlockInfoList(new AlluxioURI(path))) {
        result.add(ThriftUtils.toThrift(fileBlockInfo));
      }
      return result;
    } catch (AlluxioException e) {
      throw e.toAlluxioTException();
    }
  }

  @Override
  public long getNewBlockIdForFile(String path) throws AlluxioTException {
    try {
      return mFileSystemMaster.getNewBlockIdForFile(new AlluxioURI(path));
    } catch (AlluxioException e) {
      throw e.toAlluxioTException();
    }
  }

  @Override
  public FileInfo getStatus(String path) throws AlluxioTException {
    try {
      return ThriftUtils.toThrift(mFileSystemMaster.getFileInfo(new AlluxioURI(path)));
    } catch (AlluxioException e) {
      throw e.toAlluxioTException();
    }
  }

  @Override
  public FileInfo getStatusInternal(long fileId) throws AlluxioTException {
    try {
      return ThriftUtils.toThrift(mFileSystemMaster.getFileInfo(fileId));
    } catch (AlluxioException e) {
      throw e.toAlluxioTException();
    }
  }

  @Override
  public String getUfsAddress() {
    return mFileSystemMaster.getUfsAddress();
  }

  @Override
  public List<FileInfo> listStatus(String path) throws AlluxioTException {
    try {
      List<FileInfo> result = new ArrayList<FileInfo>();
      for (alluxio.wire.FileInfo fileInfo :
          mFileSystemMaster.getFileInfoList(new AlluxioURI(path))) {
        result.add(ThriftUtils.toThrift(fileInfo));
      }
      return result;
    } catch (AlluxioException e) {
      throw e.toAlluxioTException();
    }
  }

  @Override
  public long loadMetadata(String alluxioPath, boolean recursive)
      throws AlluxioTException, ThriftIOException {
    try {
      return mFileSystemMaster.loadMetadata(new AlluxioURI(alluxioPath), recursive);
    } catch (AlluxioException e) {
      throw e.toAlluxioTException();
    } catch (IOException e) {
      throw new ThriftIOException(e.getMessage());
    }
  }

  @Override
  public void mount(String alluxioPath, String ufsPath)
      throws AlluxioTException, ThriftIOException {
    try {
      mFileSystemMaster.mount(new AlluxioURI(alluxioPath), new AlluxioURI(ufsPath));
    } catch (AlluxioException e) {
      throw e.toAlluxioTException();
    } catch (IOException e) {
      throw new ThriftIOException(e.getMessage());
    }
  }

  @Override
  public void remove(String path, boolean recursive)
      throws AlluxioTException, ThriftIOException {
    try {
      mFileSystemMaster.deleteFile(new AlluxioURI(path), recursive);
    } catch (AlluxioException e) {
      throw e.toAlluxioTException();
    } catch (IOException e) {
      throw new ThriftIOException(e.getMessage());
    }
  }

  @Override
  public void rename(String srcPath, String dstPath)
      throws AlluxioTException, ThriftIOException {
    try {
      mFileSystemMaster.rename(new AlluxioURI(srcPath), new AlluxioURI(dstPath));
    } catch (AlluxioException e) {
      throw e.toAlluxioTException();
    } catch (IOException e) {
      throw new ThriftIOException(e.getMessage());
    }
  }

  @Override
  public void scheduleAsyncPersist(String path) throws AlluxioTException {
    try {
      mFileSystemMaster.scheduleAsyncPersistence(new AlluxioURI(path));
    } catch (FileDoesNotExistException e) {
      throw e.toAlluxioTException();
    } catch (InvalidPathException e) {
      throw e.toAlluxioTException();
    }
  }

  // TODO(calvin): Do not rely on client side options
  @Override
  public void setAttribute(String path, SetAttributeTOptions options) throws AlluxioTException {
    try {
      mFileSystemMaster.setAttribute(new AlluxioURI(path), new SetAttributeOptions(options));
    } catch (AlluxioException e) {
      throw e.toAlluxioTException();
    }
  }

  @Override
  public void unmount(String alluxioPath) throws AlluxioTException, ThriftIOException {
    try {
      mFileSystemMaster.unmount(new AlluxioURI(alluxioPath));
    } catch (AlluxioException e) {
      throw e.toAlluxioTException();
    } catch (IOException e) {
      throw new ThriftIOException(e.getMessage());
    }
  }
}
