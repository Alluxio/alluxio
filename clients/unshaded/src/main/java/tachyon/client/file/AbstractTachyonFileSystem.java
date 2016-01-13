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
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.annotation.PublicApi;
import tachyon.client.file.options.CreateOptions;
import tachyon.client.file.options.DeleteOptions;
import tachyon.client.file.options.FreeOptions;
import tachyon.client.file.options.GetInfoOptions;
import tachyon.client.file.options.ListStatusOptions;
import tachyon.client.file.options.LoadMetadataOptions;
import tachyon.client.file.options.MkdirOptions;
import tachyon.client.file.options.MountOptions;
import tachyon.client.file.options.OpenOptions;
import tachyon.client.file.options.RenameOptions;
import tachyon.client.file.options.SetStateOptions;
import tachyon.client.file.options.UnmountOptions;
import tachyon.exception.ExceptionMessage;
import tachyon.exception.FileAlreadyExistsException;
import tachyon.exception.FileDoesNotExistException;
import tachyon.exception.InvalidPathException;
import tachyon.exception.DirectoryNotEmptyException;
import tachyon.exception.TachyonException;
import tachyon.thrift.FileInfo;

/**
 * Tachyon File System client. This class should be used to interface with the Tachyon File System
 * master and supports all non IO operations. Implementing classes should provide their own IO
 * methods separate from the {@link TachyonFileSystemCore} interface.
 */
@PublicApi
public abstract class AbstractTachyonFileSystem implements TachyonFileSystemCore {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** The file system context which contains shared resources, such as the fs master client */
  // TODO(calvin): Make this private after TachyonFileSystem is cleaned up
  protected FileSystemContext mContext;

  /**
   * Constructor, currently {@link FileSystem} does not retain any state
   */
  protected AbstractTachyonFileSystem() {
    mContext = FileSystemContext.INSTANCE;
  }

  @Override
  public TachyonFile create(TachyonURI path, CreateOptions options)
      throws FileAlreadyExistsException, IOException, InvalidPathException, TachyonException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      final long fileId = masterClient.create(path.getPath(), options);
      return new TachyonFile(fileId);
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  /**
   * {@inheritDoc}
   *
   * The delete will abort on a failure, but previous deletes (if deleting more than one file) that
   * occurred will still be effective. The delete will only synchronously be propagated to the
   * master. The file metadata will not be available after this call, but the data in Tachyon or
   * under storage space may still reside until the delete is propagated and all current readers
   * have relinquished their locks.
   */
  @Override
  public void delete(TachyonFile file, DeleteOptions options)
      throws IOException, FileDoesNotExistException, DirectoryNotEmptyException, TachyonException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      masterClient.delete(file.getFileId(), options.isRecursive());
      LOG.info("Deleted file {} from both Tachyon Storage and under file system", file.getFileId());
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  /**
   * {@inheritDoc}
   *
   * This method is asynchronous and will be propagated to the workers through their heartbeats.
   */
  @Override
  public void free(TachyonFile file, FreeOptions options)
      throws IOException, FileDoesNotExistException, TachyonException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      masterClient.free(file.getFileId(), options.isRecursive());
      LOG.info("Removed file {} from Tachyon Storage", file.getFileId());
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  /**
   * {@inheritDoc}
   *
   * The file info is a snapshot of the file metadata, and the locations, last modified time, and
   * path are possibly inconsistent.
   */
  @Override
  public FileInfo getInfo(TachyonFile file, GetInfoOptions options)
      throws IOException, FileDoesNotExistException, TachyonException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      return masterClient.getStatus(file.getFileId());
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  /**
   * {@inheritDoc}
   *
   * The file infos are snapshots of the file metadata, and the locations, last modified time, and
   * path are possibly inconsistent.
   */
  @Override
  public List<FileInfo> listStatus(TachyonFile file, ListStatusOptions options)
      throws IOException, FileDoesNotExistException, TachyonException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      return masterClient.listStatus(file.getFileId());
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public TachyonFile loadMetadata(TachyonURI path, LoadMetadataOptions options)
      throws IOException, FileDoesNotExistException, TachyonException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      final long fileId = masterClient.loadMetadata(path.getPath(), options.isRecursive());
      LOG.info("Loaded file {}{}", path.getPath(), options.isRecursive() ? " recursively" : "");
      return new TachyonFile(fileId);
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public boolean mkdir(TachyonURI path, MkdirOptions options)
      throws IOException, FileAlreadyExistsException, InvalidPathException, TachyonException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      boolean result = masterClient.mkdir(path.getPath(), options);
      if (result) {
        LOG.info("Created directory {}", path.getPath());
      }
      return result;
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public boolean mount(TachyonURI tachyonPath, TachyonURI ufsPath, MountOptions options)
      throws IOException, TachyonException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      return masterClient.mount(tachyonPath, ufsPath);
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  /**
   * Resolves a {@link TachyonURI} to a {@link TachyonFile} which is used as the file handler for
   * non-create operations.
   *
   * @param path the path of the file, this should be in Tachyon space
   * @param openOptions method options
   * @return a {@link TachyonFile} which acts as a file handler for the path
   * @throws IOException if a non-Tachyon exception occurs
   * @throws InvalidPathException if there is no file at the given path
   * @throws TachyonException if an unexpected tachyon exception is thrown
   */
  public TachyonFile open(TachyonURI path, OpenOptions openOptions)
      throws IOException, InvalidPathException, TachyonException {
    TachyonFile f = openIfExists(path, openOptions);
    if (f == null) {
      throw new InvalidPathException(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(path));
    }
    return f;
  }

  @Override
  public TachyonFile openIfExists(TachyonURI path, OpenOptions openOptions)
      throws IOException, TachyonException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      long fileId = masterClient.getFileId(path.getPath());
      if (fileId == -1) {
        return null;
      }
      return new TachyonFile(fileId);
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public boolean rename(TachyonFile src, TachyonURI dst, RenameOptions options)
      throws IOException, FileDoesNotExistException, TachyonException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      boolean result = masterClient.rename(src.getFileId(), dst.getPath());
      if (result) {
        LOG.info("Renamed file {} to {}", src.getFileId(), dst.getPath());
      }
      return result;
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public void setState(TachyonFile file, SetStateOptions options)
      throws IOException, FileDoesNotExistException, InvalidPathException, TachyonException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      masterClient.setState(file.getFileId(), options);
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public boolean unmount(TachyonURI tachyonPath, UnmountOptions options)
      throws IOException, TachyonException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      return masterClient.unmount(tachyonPath);
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }
}
