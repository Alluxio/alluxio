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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.base.Preconditions;

import alluxio.Constants;
import alluxio.TachyonURI;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.TachyonException;
import alluxio.master.file.options.CompleteFileOptions;
import alluxio.master.file.options.CreateDirectoryOptions;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.master.file.options.SetAttributeOptions;
import alluxio.thrift.CompleteFileTOptions;
import alluxio.thrift.CreateDirectoryTOptions;
import alluxio.thrift.CreateFileTOptions;
import alluxio.thrift.FileBlockInfo;
import alluxio.thrift.FileInfo;
import alluxio.thrift.FileSystemMasterClientService;
import alluxio.thrift.SetAttributeTOptions;
import alluxio.thrift.TachyonTException;
import alluxio.thrift.ThriftIOException;
import alluxio.wire.ThriftUtils;

/**
 * This class is a Thrift handler for file system master RPCs invoked by a Tachyon client.
 */
@NotThreadSafe // TODO(jiri): make thread-safe (c.f. TACHYON-1664)
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
  public void completeFile(String path, CompleteFileTOptions options) throws TachyonTException {
    try {
      mFileSystemMaster.completeFile(new TachyonURI(path), new CompleteFileOptions(options));
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    }
  }

  @Override
  public void createDirectory(String path, CreateDirectoryTOptions options)
      throws TachyonTException, ThriftIOException {
    try {
      mFileSystemMaster.mkdir(new TachyonURI(path), new CreateDirectoryOptions(options));
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    } catch (IOException e) {
      throw new ThriftIOException(e.getMessage());
    }
  }

  @Override
  public void createFile(String path, CreateFileTOptions options) throws TachyonTException,
      ThriftIOException {
    try {
      mFileSystemMaster.create(new TachyonURI(path), new CreateFileOptions(options));
    } catch (IOException e) {
      throw new ThriftIOException(e.getMessage());
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    }
  }

  @Override
  public void free(String path, boolean recursive) throws TachyonTException {
    try {
      mFileSystemMaster.free(new TachyonURI(path), recursive);
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    }
  }

  @Override
  public List<FileBlockInfo> getFileBlockInfoList(String path) throws TachyonTException {
    try {
      List<FileBlockInfo> result = new ArrayList<FileBlockInfo>();
      for (alluxio.wire.FileBlockInfo fileBlockInfo :
          mFileSystemMaster.getFileBlockInfoList(new TachyonURI(path))) {
        result.add(ThriftUtils.toThrift(fileBlockInfo));
      }
      return result;
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    }
  }

  @Override
  public long getNewBlockIdForFile(String path) throws TachyonTException {
    try {
      return mFileSystemMaster.getNewBlockIdForFile(new TachyonURI(path));
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    }
  }

  @Override
  public FileInfo getStatus(String path) throws TachyonTException {
    try {
      return ThriftUtils.toThrift(mFileSystemMaster.getFileInfo(new TachyonURI(path)));
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    }
  }

  @Override
  public FileInfo getStatusInternal(long fileId) throws TachyonTException {
    try {
      return ThriftUtils.toThrift(mFileSystemMaster.getFileInfo(fileId));
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    }
  }

  @Override
  public String getUfsAddress() {
    return mFileSystemMaster.getUfsAddress();
  }

  @Override
  public List<FileInfo> listStatus(String path) throws TachyonTException {
    try {
      List<FileInfo> result = new ArrayList<FileInfo>();
      for (alluxio.wire.FileInfo fileInfo :
          mFileSystemMaster.getFileInfoList(new TachyonURI(path))) {
        result.add(ThriftUtils.toThrift(fileInfo));
      }
      return result;
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    }
  }

  @Override
  public long loadMetadata(String tachyonPath, boolean recursive)
      throws TachyonTException, ThriftIOException {
    try {
      return mFileSystemMaster.loadMetadata(new TachyonURI(tachyonPath), recursive);
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    } catch (IOException e) {
      throw new ThriftIOException(e.getMessage());
    }
  }

  @Override
  public void mount(String tachyonPath, String ufsPath)
      throws TachyonTException, ThriftIOException {
    try {
      mFileSystemMaster.mount(new TachyonURI(tachyonPath), new TachyonURI(ufsPath));
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    } catch (IOException e) {
      throw new ThriftIOException(e.getMessage());
    }
  }

  @Override
  public void remove(String path, boolean recursive)
      throws TachyonTException, ThriftIOException {
    try {
      mFileSystemMaster.deleteFile(new TachyonURI(path), recursive);
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    } catch (IOException e) {
      throw new ThriftIOException(e.getMessage());
    }
  }

  @Override
  public void rename(String srcPath, String dstPath)
      throws TachyonTException, ThriftIOException {
    try {
      mFileSystemMaster.rename(new TachyonURI(srcPath), new TachyonURI(dstPath));
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    } catch (IOException e) {
      throw new ThriftIOException(e.getMessage());
    }
  }

  @Override
  public void scheduleAsyncPersist(String path) throws TachyonTException {
    try {
      mFileSystemMaster.scheduleAsyncPersistence(new TachyonURI(path));
    } catch (FileDoesNotExistException e) {
      throw e.toTachyonTException();
    } catch (InvalidPathException e) {
      throw e.toTachyonTException();
    }
  }

  // TODO(calvin): Do not rely on client side options
  @Override
  public void setAttribute(String path, SetAttributeTOptions options) throws TachyonTException {
    try {
      mFileSystemMaster.setAttribute(new TachyonURI(path), new SetAttributeOptions(options));
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    }
  }

  @Override
  public void unmount(String tachyonPath) throws TachyonTException, ThriftIOException {
    try {
      mFileSystemMaster.unmount(new TachyonURI(tachyonPath));
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    } catch (IOException e) {
      throw new ThriftIOException(e.getMessage());
    }
  }
}
