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
public abstract class TachyonFileSystem implements TachyonFileSystemCore {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** The file system context which contains shared resources, such as the fs master client */
  private FileSystemContext mContext;

  /**
   * Constructor, currently TachyonFileSystem does not retain any state
   */
  private TachyonFileSystem() {
    mContext = FileSystemContext.INSTANCE;
  }

  /**
   * {@inheritDoc} The delete will abort on a failure, but previous deletes that occurred will still
   * be effective. The delete will only synchronously be propagated to the master. The file metadata
   * will not be available after this call, but the data in Tachyon or under storage space may still
   * reside until the delete is propagated.
   */
  @Override
  public void delete(TachyonFile file) throws IOException, FileDoesNotExistException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      masterClient.deleteFile(file.getFileId(), true);
      LOG.info(
          "Deleted file " + file.getFileId() + " from both Tachyon Storage and under file system");
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  /**
   * {@inheritDoc} This method is asynchronous and will be propagated to the workers through their
   * heartbeats.
   */
  @Override
  public void free(TachyonFile file) throws IOException, FileDoesNotExistException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      masterClient.free(file.getFileId(), true);
      LOG.info("Removed file " + file.getFileId() + " from Tachyon Storage");
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  /**
   * {@inheritDoc} The file info is a snapshot of the file metadata, and the locations, last
   * modified time, and path are possibly inconsistent.
   */
  @Override
  public FileInfo getInfo(TachyonFile file) throws IOException {
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
   * {@inheritDoc} The file infos are snapshots of the file metadata, and the locations, last
   * modified time, and path are possibly inconsistent.
   */
  @Override
  public List<FileInfo> listStatus(TachyonFile file) throws IOException, FileDoesNotExistException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      return masterClient.getFileInfoList(file.getFileId());
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  /**
   * Adds metadata about a file in the under storage system to Tachyon. Only metadata will be
   * updated and no data will be transferred. The data can be added to Tachyon space by doing an
   * operation with the cache option specified, for example reading.
   *
   * @param path the path to create the file in Tachyon
   * @param ufsPath the under storage system path of the file that will back the Tachyon file
   * @param recursive if true, the parent directories to the file in Tachyon will be created
   * @return the file id of the resulting file in Tachyon
   * @throws FileDoesNotExistException if there is no file at the given path
   * @throws IOException if the Tachyon path is invalid or the ufsPath does not exist
   */
  public long loadFileInfoFromUfs(TachyonURI path, TachyonURI ufsPath, boolean recursive)
      throws IOException, FileDoesNotExistException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      long fileId =
          masterClient.loadFileInfoFromUfs(path.getPath(), ufsPath.toString(), -1L, recursive);
      LOG.info(
          "Loaded file " + path.getPath() + " from " + ufsPath + (recursive ? " recursively" : ""));
      return fileId;
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean mkdirs(TachyonURI path) throws IOException, InvalidPathException,
      FileAlreadyExistException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      // TODO: Change this RPC's arguments
      boolean result = masterClient.createDirectory(path.getPath(), true);
      if (result) {
        LOG.info("Created directory " + path.getPath());
      }
      return result;
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public TachyonFile open(TachyonURI path) throws IOException, InvalidPathException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      return new TachyonFile(masterClient.getFileId(path.getPath()));
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setPin(TachyonFile file, boolean pinned) throws IOException,
      FileDoesNotExistException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      masterClient.setPinned(file.getFileId(), pinned);
      LOG.info(pinned ? "Pinned" : "Unpinned" + " file " + file.getFileId());
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean rename(TachyonFile src, TachyonURI dst) throws IOException,
      FileDoesNotExistException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      boolean result = masterClient.renameFile(src.getFileId(), dst.getPath());
      if (result) {
        LOG.info("Renamed file " + src.getFileId() + " to " + dst.getPath());
      }
      return result;
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }
}
