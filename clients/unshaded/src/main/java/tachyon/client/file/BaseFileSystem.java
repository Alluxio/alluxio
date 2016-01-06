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
import java.util.ArrayList;
import java.util.List;

import tachyon.TachyonURI;
import tachyon.annotation.PublicApi;
import tachyon.client.file.options.CreateDirectoryOptions;
import tachyon.client.file.options.CreateFileOptions;
import tachyon.client.file.options.DeleteOptions;
import tachyon.client.file.options.ExistsOptions;
import tachyon.client.file.options.FreeOptions;
import tachyon.client.file.options.GetStatusOptions;
import tachyon.client.file.options.ListStatusOptions;
import tachyon.client.file.options.LoadMetadataOptions;
import tachyon.client.file.options.MountOptions;
import tachyon.client.file.options.OpenFileOptions;
import tachyon.client.file.options.RenameOptions;
import tachyon.client.file.options.SetAttributeOptions;
import tachyon.client.file.options.UnmountOptions;
import tachyon.exception.DirectoryNotEmptyException;
import tachyon.exception.FileAlreadyExistsException;
import tachyon.exception.FileDoesNotExistException;
import tachyon.exception.InvalidPathException;
import tachyon.exception.TachyonException;
import tachyon.thrift.FileInfo;

/**
* Default implementation of the {@link FileSystem} interface. Developers can extend this class
* instead of implementing the interface. This implementation reads and writes data through
* {@link FileOutStream} and {@link FileInStream}. This class is thread safe.
*/
@PublicApi
public class BaseFileSystem implements FileSystem {
  @Override
  public TachyonURI createDirectory(TachyonURI path)
      throws FileAlreadyExistsException, InvalidPathException, IOException, TachyonException {
    return createDirectory(path, CreateDirectoryOptions.defaults());
  }

  @Override
  public TachyonURI createDirectory(TachyonURI path, CreateDirectoryOptions options)
      throws FileAlreadyExistsException, InvalidPathException, IOException, TachyonException {
    FileSystemMasterClient masterClient = FileSystemContext.INSTANCE.acquireMasterClient();
    try {
      return masterClient.createDirectory(path, options);
    } finally {
      FileSystemContext.INSTANCE.releaseMasterClient(masterClient);
    }
  }

  @Override
  public FileOutStream createFile(TachyonURI path)
      throws FileAlreadyExistsException, InvalidPathException, IOException, TachyonException {
    return createFile(path, CreateFileOptions.defaults());
  }

  @Override
  public FileOutStream createFile(TachyonURI path, CreateFileOptions options)
      throws FileAlreadyExistsException, InvalidPathException, IOException, TachyonException {
    FileSystemMasterClient masterClient = FileSystemContext.INSTANCE.acquireMasterClient();
    try {
      path = masterClient.createFile(path, options);
    } finally {
      FileSystemContext.INSTANCE.releaseMasterClient(masterClient);
    }
    return new FileOutStream(path, options.toOutStreamOptions());
  }

  @Override
  public void delete(TachyonURI path)
      throws DirectoryNotEmptyException, FileDoesNotExistException, IOException, TachyonException {
    delete(path, DeleteOptions.defaults());
  }

  @Override
  public void delete(TachyonURI path, DeleteOptions options)
      throws DirectoryNotEmptyException, FileDoesNotExistException, IOException, TachyonException {
    FileSystemMasterClient masterClient = FileSystemContext.INSTANCE.acquireMasterClient();
    try {
      masterClient.delete(path, options);
    } finally {
      FileSystemContext.INSTANCE.releaseMasterClient(masterClient);
    }
  }

  @Override
  public boolean exists(TachyonURI path)
      throws InvalidPathException, IOException, TachyonException {
    return exists(path, ExistsOptions.defaults());
  }

  @Override
  public boolean exists(TachyonURI path, ExistsOptions options)
      throws InvalidPathException, IOException, TachyonException {
    FileSystemMasterClient masterClient = FileSystemContext.INSTANCE.acquireMasterClient();
    try {
      // TODO(calvin): Make this more efficient
      masterClient.getFileInfo(path);
      return true;
    } catch (FileDoesNotExistException e) {
      return false;
    } finally {
      FileSystemContext.INSTANCE.releaseMasterClient(masterClient);
    }
  }

  @Override
  public void free(TachyonURI path)
      throws FileDoesNotExistException, IOException, TachyonException {
    free(path, FreeOptions.defaults());
  }

  @Override
  public void free(TachyonURI path, FreeOptions options)
      throws FileDoesNotExistException, IOException, TachyonException {
    FileSystemMasterClient masterClient = FileSystemContext.INSTANCE.acquireMasterClient();
    try {
      masterClient.free(path, options);
    } finally {
      FileSystemContext.INSTANCE.releaseMasterClient(masterClient);
    }
  }

  @Override
  public URIStatus getStatus(TachyonURI path)
      throws FileDoesNotExistException, IOException, TachyonException {
    return getStatus(path, GetStatusOptions.defaults());
  }

  @Override
  public URIStatus getStatus(TachyonURI path, GetStatusOptions options)
      throws FileDoesNotExistException, IOException, TachyonException {
    FileSystemMasterClient masterClient = FileSystemContext.INSTANCE.acquireMasterClient();
    try {
      return new URIStatus(masterClient.getFileInfo(path));
    } finally {
      FileSystemContext.INSTANCE.releaseMasterClient(masterClient);
    }
  }

  @Override
  public List<URIStatus> listStatus(TachyonURI path)
      throws FileDoesNotExistException, IOException, TachyonException {
    return listStatus(path, ListStatusOptions.defaults());
  }

  @Override
  public List<URIStatus> listStatus(TachyonURI path, ListStatusOptions options)
      throws FileDoesNotExistException, IOException, TachyonException {
    FileSystemMasterClient masterClient = FileSystemContext.INSTANCE.acquireMasterClient();
    List<FileInfo> fileInfos;
    try {
      fileInfos = masterClient.getFileInfoList(path);
    } finally {
      FileSystemContext.INSTANCE.releaseMasterClient(masterClient);
    }
    List<URIStatus> uriStatuses = new ArrayList<URIStatus>(fileInfos.size());
    for (FileInfo info : fileInfos) {
      uriStatuses.add(new URIStatus(info));
    }
    return uriStatuses;
  }

  @Override
  public TachyonURI loadMetadata(TachyonURI path)
      throws FileDoesNotExistException, IOException, TachyonException {
    return loadMetadata(path, LoadMetadataOptions.defaults());
  }

  @Override
  public TachyonURI loadMetadata(TachyonURI path, LoadMetadataOptions options)
      throws FileDoesNotExistException, IOException, TachyonException {
    FileSystemMasterClient masterClient = FileSystemContext.INSTANCE.acquireMasterClient();
    try {
      return masterClient.loadMetadata(path, options);
    } finally {
      FileSystemContext.INSTANCE.releaseMasterClient(masterClient);
    }
  }

  @Override
  public void mount(TachyonURI src, TachyonURI dst) throws IOException, TachyonException {
    mount(src, dst, MountOptions.defaults());
  }

  @Override
  public void mount(TachyonURI src, TachyonURI dst, MountOptions options)
      throws IOException, TachyonException {
    FileSystemMasterClient masterClient = FileSystemContext.INSTANCE.acquireMasterClient();
    try {
      masterClient.mount(src, dst);
    } finally {
      FileSystemContext.INSTANCE.releaseMasterClient(masterClient);
    }
  }

  @Override
  public FileInStream openFile(TachyonURI path)
      throws FileDoesNotExistException, IOException, TachyonException {
    return openFile(path, OpenFileOptions.defaults());
  }

  @Override
  public FileInStream openFile(TachyonURI path, OpenFileOptions options)
      throws FileDoesNotExistException, IOException, TachyonException {
    URIStatus status = getStatus(path);
    return new FileInStream(status.getInfo(), options.toInStreamOptions());
  }

  @Override
  public void rename(TachyonURI src, TachyonURI dst)
      throws FileDoesNotExistException, IOException, TachyonException {
    rename(src, dst, RenameOptions.defaults());
  }

  @Override
  public void rename(TachyonURI src, TachyonURI dst, RenameOptions options)
      throws FileDoesNotExistException, IOException, TachyonException {
    FileSystemMasterClient masterClient = FileSystemContext.INSTANCE.acquireMasterClient();
    try {
      masterClient.rename(src, dst);
    } finally {
      FileSystemContext.INSTANCE.releaseMasterClient(masterClient);
    }
  }

  @Override
  public void setAttribute(TachyonURI path)
      throws FileDoesNotExistException, IOException, TachyonException {
    setAttribute(path, SetAttributeOptions.defaults());
  }

  @Override
  public void setAttribute(TachyonURI path, SetAttributeOptions options)
      throws FileDoesNotExistException, IOException, TachyonException {
    FileSystemMasterClient masterClient = FileSystemContext.INSTANCE.acquireMasterClient();
    try {
      masterClient.setAttribute(path, options);
    } finally {
      FileSystemContext.INSTANCE.releaseMasterClient(masterClient);
    }
  }

  @Override
  public void unmount(TachyonURI path) throws IOException, TachyonException {
    unmount(path, UnmountOptions.defaults());
  }

  @Override
  public void unmount(TachyonURI path, UnmountOptions options)
      throws IOException, TachyonException {
    FileSystemMasterClient masterClient = FileSystemContext.INSTANCE.acquireMasterClient();
    try {
      masterClient.unmount(path);
    } finally {
      FileSystemContext.INSTANCE.releaseMasterClient(masterClient);
    }
  }
}
