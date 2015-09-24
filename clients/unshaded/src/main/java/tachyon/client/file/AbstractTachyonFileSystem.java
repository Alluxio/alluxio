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
import tachyon.client.FileSystemMasterClient;
import tachyon.client.options.CreateOptions;
import tachyon.client.options.DeleteOptions;
import tachyon.client.options.FreeOptions;
import tachyon.client.options.GetInfoOptions;
import tachyon.client.options.ListStatusOptions;
import tachyon.client.options.LoadMetadataOptions;
import tachyon.client.options.MkdirOptions;
import tachyon.client.options.OpenOptions;
import tachyon.client.options.RenameOptions;
import tachyon.client.options.SetStateOptions;
import tachyon.exception.TachyonException;
import tachyon.exception.TachyonExceptionType;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.FileInfo;
import tachyon.thrift.InvalidPathException;

/**
 * Tachyon File System client. This class should be used to interface with the Tachyon File
 * System master and supports all non IO operations. Implementing classes should provide their
 * own IO methods separate from the {@link TachyonFileSystemCore} interface.
 */
@PublicApi
public abstract class AbstractTachyonFileSystem implements TachyonFileSystemCore {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** The file system context which contains shared resources, such as the fs master client */
  // TODO (calvin): Make this private after StreamingTachyonFileSystem is cleaned up
  protected FileSystemContext mContext;

  /**
   * Constructor, currently TachyonFileSystem does not retain any state
   */
  protected AbstractTachyonFileSystem() {
    mContext = FileSystemContext.INSTANCE;
  }

  @Override
  public long create(TachyonURI path, CreateOptions options) throws IOException, TachyonException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      long fileId =
          masterClient.createFile(path.getPath(), options.getBlockSize(), options.isRecursive(),
              options.getTTL());
      return fileId;
    } catch (BlockInfoException e) {
      throw new TachyonException(e, TachyonExceptionType.FILE_ALREADY_EXISTS);
    } catch (FileAlreadyExistException e) {
      throw new TachyonException(e, TachyonExceptionType.FILE_ALREADY_EXISTS);
    } catch (InvalidPathException e) {
      throw new TachyonException(e, TachyonExceptionType.INVALID_PATH);
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  /**
   * {@inheritDoc}
   *
   * The delete will abort on a failure, but previous deletes (if deleting more than
   * one file) that occurred will still be effective. The delete will only synchronously be
   * propagated to the master. The file metadata will not be available after this call, but the data
   * in Tachyon or under storage space may still reside until the delete is propagated and all
   * current readers have relinquished their locks.
   */
  @Override
  public void delete(TachyonFile file, DeleteOptions options) throws IOException, TachyonException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      masterClient.deleteFile(file.getFileId(), options.isRecursive());
      LOG.info(
          "Deleted file " + file.getFileId() + " from both Tachyon Storage and under file system");
    } catch (FileDoesNotExistException e) {
      throw new TachyonException(e, TachyonExceptionType.FILE_DOES_NOT_EXIST);
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
  public void free(TachyonFile file, FreeOptions options) throws IOException, TachyonException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      masterClient.free(file.getFileId(), options.isRecursive());
      LOG.info("Removed file " + file.getFileId() + " from Tachyon Storage");
    } catch (FileDoesNotExistException e) {
      throw new TachyonException(e, TachyonExceptionType.FILE_DOES_NOT_EXIST);
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
  public FileInfo getInfo(TachyonFile file, GetInfoOptions options) throws IOException,
      TachyonException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      return masterClient.getFileInfo(file.getFileId());
    } catch (FileDoesNotExistException e) {
      return null;
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
  public List<FileInfo> listStatus(TachyonFile file, ListStatusOptions options) throws IOException,
      TachyonException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      return masterClient.getFileInfoList(file.getFileId());
    } catch (FileDoesNotExistException e) {
      throw new TachyonException(e, TachyonExceptionType.FILE_DOES_NOT_EXIST);
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }


  /**
   * {@inheritDoc}
   *
   * To add the data into Tachyon space perform an operation with the cache option specified, for
   * example the load command of the Tachyon Shell.
   */
  @Override
  public long loadMetadata(TachyonURI path, TachyonURI ufsPath, LoadMetadataOptions options)
      throws IOException, TachyonException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      long fileId =
          masterClient.loadFileInfoFromUfs(path.getPath(), ufsPath.toString(),
              options.isRecursive());
      LOG.info("Loaded file " + path.getPath() + " from " + ufsPath
          + (options.isRecursive() ? " recursively" : ""));
      return fileId;
    } catch (FileDoesNotExistException e) {
      throw new TachyonException(e, TachyonExceptionType.FILE_DOES_NOT_EXIST);
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public boolean mkdir(TachyonURI path, MkdirOptions options) throws IOException, TachyonException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      boolean result = masterClient.createDirectory(path.getPath(), options.isRecursive());
      if (result) {
        LOG.info("Created directory " + path.getPath());
      }
      return result;
    } catch (FileAlreadyExistException e) {
      throw new TachyonException(e, TachyonExceptionType.FILE_ALREADY_EXISTS);
    } catch (InvalidPathException e) {
      throw new TachyonException(e, TachyonExceptionType.INVALID_PATH);
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public TachyonFile open(TachyonURI path, OpenOptions openOptions) throws IOException,
      TachyonException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      return new TachyonFile(masterClient.getFileId(path.getPath()));
    } catch (InvalidPathException e) {
      throw new TachyonException(e, TachyonExceptionType.INVALID_PATH);
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public boolean rename(TachyonFile src, TachyonURI dst, RenameOptions options) throws IOException,
      TachyonException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      boolean result = masterClient.renameFile(src.getFileId(), dst.getPath());
      if (result) {
        LOG.info("Renamed file " + src.getFileId() + " to " + dst.getPath());
      }
      return result;
    } catch (FileDoesNotExistException e) {
      throw new TachyonException(e, TachyonExceptionType.FILE_DOES_NOT_EXIST);
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public void setState(TachyonFile file, SetStateOptions options) throws IOException,
      TachyonException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    Boolean pinned = options.getPinned();
    try {
      if (pinned != null) {
        masterClient.setPinned(file.getFileId(), pinned);
        LOG.info(pinned ? "Pinned" : "Unpinned" + " file " + file.getFileId());
      }
    } catch (FileDoesNotExistException e) {
      throw new TachyonException(e, TachyonExceptionType.FILE_DOES_NOT_EXIST);
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }
}
