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
import tachyon.exception.FileAlreadyExistsException;
import tachyon.exception.FileDoesNotExistException;
import tachyon.exception.InvalidPathException;
import tachyon.exception.TachyonException;
import tachyon.exception.TachyonExceptionType;
import tachyon.thrift.FileInfo;

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
  public TachyonFile create(TachyonURI path, CreateOptions options)
      throws FileAlreadyExistsException, IOException, InvalidPathException, TachyonException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      final long fileId = masterClient.create(path.getPath(), options.getBlockSize(),
          options.isRecursive(), options.getTTL());
      return new TachyonFile(fileId);
    } catch (TachyonException e) {
      if (e.getType() == TachyonExceptionType.BLOCK_INFO) {
        throw new FileAlreadyExistsException(e.getMessage(), e);
      } else {
        TachyonException.unwrap(e, FileAlreadyExistsException.class);
        TachyonException.unwrap(e, InvalidPathException.class);
        throw e;
      }
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
  public void delete(TachyonFile file, DeleteOptions options) throws IOException,
      FileDoesNotExistException, TachyonException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      masterClient.deleteFile(file.getFileId(), options.isRecursive());
      LOG.info("Deleted file " + file.getFileId()
          + " from both Tachyon Storage and under file system");
    } catch (TachyonException e) {
      TachyonException.unwrap(e, FileDoesNotExistException.class);
      throw e;
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
  public void free(TachyonFile file, FreeOptions options) throws IOException,
      FileDoesNotExistException, TachyonException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      masterClient.free(file.getFileId(), options.isRecursive());
      LOG.info("Removed file " + file.getFileId() + " from Tachyon Storage");
    } catch (TachyonException e) {
      TachyonException.unwrap(e, FileDoesNotExistException.class);
      throw e;
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
    FileDoesNotExistException, TachyonException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      return masterClient.getFileInfo(file.getFileId());
    } catch (TachyonException e) {
      TachyonException.unwrap(e, FileDoesNotExistException.class);
      throw e;
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
      FileDoesNotExistException, TachyonException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      return masterClient.getFileInfoList(file.getFileId());
    } catch (TachyonException e) {
      TachyonException.unwrap(e, FileDoesNotExistException.class);
      throw e;
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public TachyonFile loadMetadata(TachyonURI path, LoadMetadataOptions options)
      throws IOException, FileDoesNotExistException, TachyonException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      final long fileId =
          masterClient.loadMetadata(path.getPath(), options.isRecursive());
      LOG.info("Loaded file " + path.getPath() + (options.isRecursive() ? " recursively" : ""));
      return new TachyonFile(fileId);
    } catch (TachyonException e) {
      TachyonException.unwrap(e, FileDoesNotExistException.class);
      throw e;
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public boolean mkdir(TachyonURI path, MkdirOptions options) throws IOException,
      FileAlreadyExistsException, InvalidPathException, TachyonException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      boolean result = masterClient.mkdir(path.getPath(), options.isRecursive());
      if (result) {
        LOG.info("Created directory " + path.getPath());
      }
      return result;
    } catch (TachyonException e) {
      TachyonException.unwrap(e, FileAlreadyExistsException.class);
      TachyonException.unwrap(e, InvalidPathException.class);
      throw e;
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

  @Override
  public TachyonFile open(TachyonURI path, OpenOptions openOptions) throws IOException {
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
  public boolean rename(TachyonFile src, TachyonURI dst, RenameOptions options) throws IOException,
      FileDoesNotExistException, TachyonException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      boolean result = masterClient.renameFile(src.getFileId(), dst.getPath());
      if (result) {
        LOG.info("Renamed file " + src.getFileId() + " to " + dst.getPath());
      }
      return result;
    } catch (TachyonException e) {
      TachyonException.unwrap(e, FileDoesNotExistException.class);
      throw e;
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public void setState(TachyonFile file, SetStateOptions options) throws IOException,
      FileDoesNotExistException, TachyonException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    Boolean pinned = options.getPinned();
    Long ttl = options.getTTL();
    try {
      if (pinned != null) {
        masterClient.setPinned(file.getFileId(), pinned);
        LOG.info(pinned ? "Pinned" : "Unpinned" + " file " + file.getFileId());
      }
      if (ttl != null) {
        masterClient.setTTL(file.getFileId(), ttl);
        LOG.info("Set new TTL " + ttl + " for file " + file.getFileId());
      }
    } catch (TachyonException e) {
      TachyonException.unwrap(e, FileDoesNotExistException.class);
      throw e;
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
