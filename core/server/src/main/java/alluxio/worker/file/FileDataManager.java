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

package alluxio.worker.file;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import alluxio.Constants;
import alluxio.Sessions;
import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.io.BufferUtils;
import alluxio.util.io.PathUtils;
import alluxio.wire.FileInfo;
import alluxio.worker.WorkerContext;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.io.BlockReader;

/**
 * Responsible for storing files into under file system.
 */
@NotThreadSafe // TODO(jiri): make thread-safe (c.f. TACHYON-1624)
public final class FileDataManager {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final UnderFileSystem mUfs;

  /** Block worker handler for access block info */
  private final BlockWorker mBlockWorker;

  /** The file being persisted, and the inner map tracks the block id to lock id */
  @GuardedBy("mLock")
  // the file being persisted,
  private final Map<Long, Map<Long, Long>> mPersistingInProgressFiles;

  /** The file are persisted, but not sent back to master for confirmation yet. */
  @GuardedBy("mLock")
  private final Set<Long> mPersistedFiles;

  private final Configuration mConfiguration;
  private final Object mLock = new Object();

  /**
   * Creates a new instance of {@link FileDataManager}.
   *
   * @param blockWorker the block worker handle
   */
  public FileDataManager(BlockWorker blockWorker) {
    mBlockWorker = Preconditions.checkNotNull(blockWorker);
    mPersistingInProgressFiles = Maps.newHashMap();
    mPersistedFiles = Sets.newHashSet();
    mConfiguration = WorkerContext.getConf();
    // Create Under FileSystem Client
    String ufsAddress = mConfiguration.get(Constants.UNDERFS_ADDRESS);
    mUfs = UnderFileSystem.get(ufsAddress, mConfiguration);
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
    String ufsRoot = mConfiguration.get(Constants.UNDERFS_ADDRESS);
    FileInfo fileInfo = mBlockWorker.getFileInfo(fileId);
    String dstPath = PathUtils.concatPath(ufsRoot, fileInfo.getPath());

    return mUfs.exists(dstPath);
  }

  /**
   * Locks all the blocks of a given file Id.
   *
   * @param fileId the id of the file
   * @param blockIds the ids of the file's blocks
   * @throws IOException when an I/O exception occurs
   */
  public void lockBlocks(long fileId, List<Long> blockIds) throws IOException {
    Map<Long, Long> blockIdToLockId = Maps.newHashMap();
    List<Throwable> errors = new ArrayList<Throwable>();
    synchronized (mLock) {
      if (mPersistingInProgressFiles.containsKey(fileId)) {
        throw new IOException("the file " + fileId + " is already being persisted");
      }
      try {
        // lock all the blocks to prevent any eviction
        for (long blockId : blockIds) {
          long lockId = mBlockWorker.lockBlock(Sessions.CHECKPOINT_SESSION_ID, blockId);
          blockIdToLockId.put(blockId, lockId);
        }
      } catch (BlockDoesNotExistException e) {
        errors.add(e);
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
          errorStr.append("failed to lock all blocks of file ").append(fileId).append("\n");
          for (Throwable error : errors) {
            errorStr.append(error).append('\n');
          }
          throw new IOException(errorStr.toString());
        }
      }
      mPersistingInProgressFiles.put(fileId, blockIdToLockId);
    }
  }

  /**
   * Persists the blocks of a file into the under file system.
   *
   * @param fileId the id of the file
   * @param blockIds the list of block ids
   * @throws IOException if the file persistence fails
   */
  public void persistFile(long fileId, List<Long> blockIds) throws IOException {
    Map<Long, Long> blockIdToLockId;
    synchronized (mLock) {
      blockIdToLockId = mPersistingInProgressFiles.get(fileId);
      if (blockIdToLockId == null || !blockIdToLockId.keySet().equals(Sets.newHashSet(blockIds))) {
        throw new IOException("Not all the blocks of file " + fileId + " are blocked");
      }
    }

    String dstPath = prepareUfsFilePath(fileId);
    OutputStream outputStream = mUfs.create(dstPath);
    final WritableByteChannel outputChannel = Channels.newChannel(outputStream);

    List<Throwable> errors = new ArrayList<Throwable>();
    try {
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
        } catch (BlockDoesNotExistException e) {
          errors.add(e);
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
    String ufsRoot = mConfiguration.get(Constants.UNDERFS_ADDRESS);
    FileInfo fileInfo = mBlockWorker.getFileInfo(fileId);
    AlluxioURI uri = new AlluxioURI(fileInfo.getPath());
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
