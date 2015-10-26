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

package tachyon.master.file;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import tachyon.TachyonURI;
import tachyon.exception.TachyonException;
import tachyon.master.file.options.CreateOptions;
import tachyon.master.file.options.MkdirOptions;
import tachyon.thrift.CreateTOptions;
import tachyon.thrift.FileBlockInfo;
import tachyon.thrift.FileInfo;
import tachyon.thrift.FileSystemMasterService;
import tachyon.thrift.MkdirTOptions;
import tachyon.thrift.TachyonTException;
import tachyon.thrift.ThriftIOException;

public final class FileSystemMasterServiceHandler implements FileSystemMasterService.Iface {
  private final FileSystemMaster mFileSystemMaster;

  public FileSystemMasterServiceHandler(FileSystemMaster fileSystemMaster) {
    mFileSystemMaster = fileSystemMaster;
  }

  @Override
  public void completeFile(long fileId) throws TachyonTException {
    try {
      mFileSystemMaster.completeFile(fileId);
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    }
  }

  @Override
  public long create(String path, CreateTOptions options) throws TachyonTException,
      ThriftIOException {
    try {
      return mFileSystemMaster.create(new TachyonURI(path), new CreateOptions(options));
    } catch (IOException e) {
      throw new ThriftIOException(e.getMessage());
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    }
  }

  @Override
  public boolean free(long fileId, boolean recursive) throws TachyonTException {
    try {
      return mFileSystemMaster.free(fileId, recursive);
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    }
  }

  @Override
  public long getFileId(String path) throws ThriftIOException {
    try {
      return mFileSystemMaster.getFileId(new TachyonURI(path));
    } catch (IOException e) {
      throw new ThriftIOException(e.getMessage());
    }
  }

  @Override
  public FileInfo getFileInfo(long fileId) throws TachyonTException {
    try {
      return mFileSystemMaster.getFileInfo(fileId);
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    }
  }

  @Override
  public List<FileInfo> getFileInfoList(long fileId) throws TachyonTException {
    try {
      return mFileSystemMaster.getFileInfoList(fileId);
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    }
  }

  @Override
  public FileBlockInfo getFileBlockInfo(long fileId, int fileBlockIndex) throws TachyonTException {
    try {
      return mFileSystemMaster.getFileBlockInfo(fileId, fileBlockIndex);
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    }
  }

  @Override
  public List<FileBlockInfo> getFileBlockInfoList(long fileId) throws TachyonTException {
    try {
      return mFileSystemMaster.getFileBlockInfoList(fileId);
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    }
  }

  @Override
  public long getNewBlockIdForFile(long fileId) throws TachyonTException {
    try {
      return mFileSystemMaster.getNewBlockIdForFile(fileId);
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    }
  }

  @Override
  public String getUfsAddress() {
    return mFileSystemMaster.getUfsAddress();
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
  public boolean mkdir(String path, MkdirTOptions options) throws TachyonTException,
      ThriftIOException {
    try {
      mFileSystemMaster.mkdir(new TachyonURI(path), new MkdirOptions(options));
      return true;
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    } catch (IOException e) {
      throw new ThriftIOException(e.getMessage());
    }
  }

  @Override
  public boolean mount(String tachyonPath, String ufsPath)
      throws TachyonTException, ThriftIOException {
    try {
      return mFileSystemMaster.mount(new TachyonURI(tachyonPath), new TachyonURI(ufsPath));
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    } catch (IOException e) {
      throw new ThriftIOException(e.getMessage());
    }
  }

  @Override
  public boolean persistFile(long fileId, long length) throws TachyonTException {
    try {
      return mFileSystemMaster.persistFile(fileId, length);
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    }
  }

  @Override
  public boolean remove(long fileId, boolean recursive)
      throws TachyonTException, ThriftIOException {
    try {
      return mFileSystemMaster.deleteFile(fileId, recursive);
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    } catch (IOException e) {
      throw new ThriftIOException(e.getMessage());
    }
  }

  @Override
  public void setTTL(long fileId, long ttl) throws TachyonTException {
    try {
      mFileSystemMaster.setTTL(fileId, ttl);
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    }
  }

  public boolean rename(long fileId, String dstPath)
      throws TachyonTException, ThriftIOException {
    try {
      return mFileSystemMaster.rename(fileId, new TachyonURI(dstPath));
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    } catch (IOException e) {
      throw new ThriftIOException(e.getMessage());
    }
  }

  @Override
  public void setPinned(long fileId, boolean pinned) throws TachyonTException {
    try {
      mFileSystemMaster.setPinned(fileId, pinned);
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    }
  }

  @Override
  public void reportLostFile(long fileId) throws TachyonTException {
    try {
      mFileSystemMaster.reportLostFile(fileId);
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    }
  }

  @Override
  public boolean unmount(String tachyonPath) throws TachyonTException, ThriftIOException {
    try {
      return mFileSystemMaster.unmount(new TachyonURI(tachyonPath));
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    } catch (IOException e) {
      throw new ThriftIOException(e.getMessage());
    }
  }

  @Override
  public Set<Long> workerGetPinIdList() {
    return mFileSystemMaster.getPinIdList();
  }

}
