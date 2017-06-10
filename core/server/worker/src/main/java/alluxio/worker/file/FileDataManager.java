/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.file;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.Sessions;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.security.authorization.Mode;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.CreateOptions;
import alluxio.util.io.BufferUtils;
import alluxio.wire.FileInfo;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.meta.BlockMeta;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Responsible for storing files into under file system.
 */
@NotThreadSafe // TODO(jiri): make thread-safe (c.f. ALLUXIO-1624)
public final class FileDataManager {
  private static final Logger LOG = LoggerFactory.getLogger(FileDataManager.class);

  /** Block worker handler for access block info. */
  private final BlockWorker mBlockWorker;

  /** The files being persisted, keyed by fileId,
   * and the inner map tracks the block id to lock id. */
  @GuardedBy("mLock")
  // the file being persisted,
  private final Map<Long, Map<Long, Long>> mPersistingInProgressFiles;

  /** The file are persisted, but not sent back to master for confirmation yet. */
  @GuardedBy("mLock")
  private final Set<Long> mPersistedFiles;

  private final Object mLock = new Object();

  /** A per worker rate limiter to throttle async persistence. */
  private final RateLimiter mPersistenceRateLimiter;
  /** The manager for all ufs. */
  private final UfsManager mUfsManager;

  /**
   * Creates a new instance of {@link FileDataManager}.
   *
   * @param blockWorker the block worker handle
   * @param persistenceRateLimiter a per worker rate limiter to throttle async persistence
   * @param ufsManager the ufs manager
   */
  public FileDataManager(BlockWorker blockWorker, RateLimiter persistenceRateLimiter,
      UfsManager ufsManager) {
    mBlockWorker = Preconditions.checkNotNull(blockWorker, "blockWorker");
    mPersistingInProgressFiles = new HashMap<>();
    mPersistedFiles = new HashSet<>();
    mPersistenceRateLimiter = persistenceRateLimiter;
    mUfsManager = ufsManager;
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
    } catch (Exception e) {
      LOG.warn("Failed to check if file {} exists in under storage system: {}",
               fileId, e.getMessage());
      LOG.debug("Exception: ", e);
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
   */
  private synchronized boolean fileExistsInUfs(long fileId) throws IOException {
    FileInfo fileInfo = mBlockWorker.getFileInfo(fileId);
    String dstPath = fileInfo.getUfsPath();
    UnderFileSystem ufs = mUfsManager.get(fileInfo.getMountId()).getUfs();
    return ufs.isFile(dstPath);
  }

  /**
   * Locks all the blocks of a given file Id.
   *
   * @param fileId the id of the file
   * @param blockIds the ids of the file's blocks
   */
  public void lockBlocks(long fileId, List<Long> blockIds) throws IOException {
    Map<Long, Long> blockIdToLockId = new HashMap<>();
    List<Throwable> errors = new ArrayList<>();
    synchronized (mLock) {
      if (mPersistingInProgressFiles.containsKey(fileId)) {
        throw new IOException("the file " + fileId + " is already being persisted");
      }
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
    synchronized (mLock) {
      mPersistingInProgressFiles.put(fileId, blockIdToLockId);
    }
  }

  /**
   * Persists the blocks of a file into the under file system.
   *
   * @param fileId the id of the file
   * @param blockIds the list of block ids
   */
  public void persistFile(long fileId, List<Long> blockIds) throws AlluxioException, IOException {
    Map<Long, Long> blockIdToLockId;
    synchronized (mLock) {
      blockIdToLockId = mPersistingInProgressFiles.get(fileId);
      if (blockIdToLockId == null || !blockIdToLockId.keySet().equals(new HashSet<>(blockIds))) {
        throw new IOException("Not all the blocks of file " + fileId + " are locked");
      }
    }

    String dstPath = prepareUfsFilePath(fileId);
    FileInfo fileInfo = mBlockWorker.getFileInfo(fileId);
    UnderFileSystem ufs = mUfsManager.get(fileInfo.getMountId()).getUfs();
    OutputStream outputStream = ufs.create(dstPath, CreateOptions.defaults()
        .setOwner(fileInfo.getOwner()).setGroup(fileInfo.getGroup())
        .setMode(new Mode((short) fileInfo.getMode())));
    final WritableByteChannel outputChannel = Channels.newChannel(outputStream);

    List<Throwable> errors = new ArrayList<>();
    try {
      for (long blockId : blockIds) {
        long lockId = blockIdToLockId.get(blockId);

        if (Configuration.getBoolean(PropertyKey.WORKER_FILE_PERSIST_RATE_LIMIT_ENABLED)) {
          BlockMeta blockMeta =
              mBlockWorker.getBlockMeta(Sessions.CHECKPOINT_SESSION_ID, blockId, lockId);
          mPersistenceRateLimiter.acquire((int) blockMeta.getBlockSize());
        }

        // obtain block reader
        BlockReader reader =
            mBlockWorker.readBlockRemote(Sessions.CHECKPOINT_SESSION_ID, blockId, lockId);

        // write content out
        ReadableByteChannel inputChannel = reader.getChannel();
        BufferUtils.fastCopy(inputChannel, outputChannel);
        reader.close();
      }
    } catch (BlockDoesNotExistException | InvalidWorkerStateException e) {
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

      // Process any errors
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
   */
  private String prepareUfsFilePath(long fileId) throws AlluxioException, IOException {
    FileInfo fileInfo = mBlockWorker.getFileInfo(fileId);
    AlluxioURI alluxioPath = new AlluxioURI(fileInfo.getPath());
    FileSystem fs = FileSystem.Factory.get();
    URIStatus status = fs.getStatus(alluxioPath);
    String ufsPath = status.getUfsPath();
    UnderFileSystem ufs = mUfsManager.get(fileInfo.getMountId()).getUfs();
    UnderFileSystemUtils.prepareFilePath(alluxioPath, ufsPath, fs, ufs);
    return ufsPath;
  }

  /**
   * @return the persisted file
   */
  public List<Long> getPersistedFiles() {
    synchronized (mLock) {
      return ImmutableList.copyOf(mPersistedFiles);
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
