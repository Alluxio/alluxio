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

package tachyon.worker.file;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import tachyon.Constants;
import tachyon.Sessions;
import tachyon.TachyonURI;
import tachyon.conf.TachyonConf;
import tachyon.exception.BlockDoesNotExistException;
import tachyon.exception.InvalidWorkerStateException;
import tachyon.thrift.FileInfo;
import tachyon.underfs.UnderFileSystem;
import tachyon.util.io.BufferUtils;
import tachyon.util.io.PathUtils;
import tachyon.worker.WorkerContext;
import tachyon.worker.block.BlockWorker;
import tachyon.worker.block.io.BlockReader;

/**
 * Responsible for Storing files into under file system.
 */
public final class FileDataManager {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final UnderFileSystem mUfs;
  /** Block data manager for access block info */
  private final BlockWorker mBlockWorker;

  // the file being persisted
  private final Set<Long> mPersistingInProgressFiles;
  // the file are persisted, but not sent back to master for confirmation yet
  private final Set<Long> mPersistedFiles;
  private final TachyonConf mTachyonConf;
  private final Object mLock = new Object();

  /**
   * Creates a new instance of {@link FileDataManager}.
   *
   * @param blockDataManager a block data manager handle
   */
  public FileDataManager(BlockWorker blockWorker) {
    mBlockWorker = Preconditions.checkNotNull(blockWorker);
    mPersistingInProgressFiles = Sets.newHashSet();
    mPersistedFiles = Sets.newHashSet();
    mTachyonConf = WorkerContext.getConf();
    // Create Under FileSystem Client
    String ufsAddress = mTachyonConf.get(Constants.UNDERFS_ADDRESS);
    mUfs = UnderFileSystem.get(ufsAddress, mTachyonConf);
  }

  /**
   * Checks if the given file is being persisted.
   *
   * @param fileId the file id
   * @return true if the file is being persisted, false otherwise
   */
  private boolean isFilePersisting(long fileId) {
    synchronized (mLock) {
      return mPersistedFiles.contains(fileId);
    }
  }

  /**
   * Checks if the given file needs persistence.
   *
   * @param fileId the file id
   * @return false if the file is being persisted, or is already persisted; otherwise true
   */
  public boolean needPersistence(long fileId) {
    if (isFilePersisting(fileId) || isFilePersisted(fileId)) {
      return false;
    }

    try {
      if (fileExistsInUfs(fileId)) {
        // mark as persisted
        addPersistedFile(fileId);
        return false;
      }
    } catch (IOException e) {
      LOG.error("Failed to check if file {} exists in under storage system", fileId, e);
    }
    return true;
  }

  /**
   * Checks if the given file is persisted.
   *
   * @param fileId the file id
   * @return true if the file is being persisted, false otherwise
   */
  public boolean isFilePersisted(long fileId) {
    synchronized (mLock) {
      return mPersistedFiles.contains(fileId);
    }
  }

  /**
   * Adds a file as persisted.
   *
   * @param fileId the file id
   */
  private void addPersistedFile(long fileId) {
    synchronized (mLock) {
      mPersistedFiles.add(fileId);
    }
  }

  /**
   * Checks if the given file exists in the under storage system.
   *
   * @param fileId the file id
   * @return true if the file exists in under storage system, false otherwise
   * @throws IOException an I/O exception occurs
   */
  private synchronized boolean fileExistsInUfs(long fileId) throws IOException {
    String ufsRoot = mTachyonConf.get(Constants.UNDERFS_ADDRESS);
    FileInfo fileInfo = mBlockWorker.getFileInfo(fileId);
    String dstPath = PathUtils.concatPath(ufsRoot, fileInfo.getPath());

    return mUfs.exists(dstPath);
  }

  /**
   * Persists the blocks of a file into the under file system.
   *
   * @param fileId the id of the file
   * @param blockIds the list of block ids
   * @throws IOException if the file persistence fails
   */
  public void persistFile(long fileId, List<Long> blockIds) throws IOException {
    synchronized (mLock) {
      mPersistingInProgressFiles.add(fileId);
    }

    String dstPath = prepareUfsFilePath(fileId);
    OutputStream outputStream = mUfs.create(dstPath);
    final WritableByteChannel outputChannel = Channels.newChannel(outputStream);

    Map<Long, Long> blockIdToLockId = Maps.newHashMap();
    List<Throwable> errors = new ArrayList<Throwable>();
    try {
      // lock all the blocks to prevent any eviction
      for (long blockId : blockIds) {
        long lockId = mBlockWorker.lockBlock(Sessions.CHECKPOINT_SESSION_ID, blockId);
        blockIdToLockId.put(blockId, lockId);
      }

      for (long blockId : blockIds) {
        long lockId = blockIdToLockId.get(blockId);

        // obtain block reader
        BlockReader reader =
            mBlockWorker.readBlockRemote(Sessions.CHECKPOINT_SESSION_ID, blockId, lockId);

        // write content out
        ReadableByteChannel inputChannel = reader.getChannel();
        BufferUtils.fastCopy(inputChannel, outputChannel);
        reader.close();
      }
    } catch (BlockDoesNotExistException e) {
      errors.add(e);
    } catch (InvalidWorkerStateException e) {
      errors.add(e);
    } finally {
      // make sure all the locks are released
      for (long lockId : blockIdToLockId.values()) {
        try {
          mBlockWorker.unlockBlock(lockId);
        } catch (BlockDoesNotExistException bdnee) {
          errors.add(bdnee);
        }
      }

      if (!errors.isEmpty()) {
        StringBuilder errorStr = new StringBuilder();
        errorStr.append("the blocks of file").append(fileId).append(" are failed to persist\n");
        for (Throwable e : errors) {
          errorStr.append(e).append('\n');
        }
        throw new IOException(errorStr.toString());
      }
    }

    outputStream.flush();
    outputChannel.close();
    outputStream.close();
    synchronized (mLock) {
      mPersistingInProgressFiles.remove(fileId);
      mPersistedFiles.add(fileId);
    }
  }

  /**
   * Prepares the destination file path of the given file id. Also creates the parent folder if it
   * does not exist.
   *
   * @param fileId the file id
   * @return the path for persistence
   * @throws IOException if the folder creation fails
   */
  private String prepareUfsFilePath(long fileId) throws IOException {
    String ufsRoot = mTachyonConf.get(Constants.UNDERFS_ADDRESS);
    FileInfo fileInfo = mBlockWorker.getFileInfo(fileId);
    TachyonURI uri = new TachyonURI(fileInfo.getPath());
    String dstPath = PathUtils.concatPath(ufsRoot, fileInfo.getPath());
    LOG.info("persist file {} at {}", fileId, dstPath);
    String parentPath = PathUtils.concatPath(ufsRoot, uri.getParent().getPath());
    // creates the parent folder if it does not exist
    if (!mUfs.exists(parentPath) && !mUfs.mkdirs(parentPath, true)) {
      throw new IOException("Failed to create " + parentPath);
    }
    return dstPath;
  }

  /**
   * @return the persisted file
   */
  public List<Long> getPersistedFiles() {
    List<Long> toReturn = Lists.newArrayList();
    synchronized (mLock) {
      toReturn.addAll(mPersistedFiles);
      return toReturn;
    }
  }

  /**
   * Clears the given persisted files stored in {@link #mPersistedFiles}.
   *
   * @param persistedFiles the list of persisted files to clear
   */
  public void clearPersistedFiles(List<Long> persistedFiles) {
    synchronized (mLock) {
      mPersistedFiles.removeAll(persistedFiles);
    }
  }
}
