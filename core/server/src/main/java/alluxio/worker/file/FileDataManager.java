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
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.Sessions;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.collections.Pair;
import alluxio.exception.AlluxioException;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.security.authorization.Permission;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.CreateOptions;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.util.io.BufferUtils;
import alluxio.util.io.PathUtils;
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
import java.util.Stack;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Responsible for storing files into under file system.
 */
@NotThreadSafe // TODO(jiri): make thread-safe (c.f. ALLUXIO-1624)
public final class FileDataManager {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final UnderFileSystem mUfs;

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

  /**
   * Creates a new instance of {@link FileDataManager}.
   *
   * @param blockWorker the block worker handle
   * @param ufs the under file system to persist files to
   * @param persistenceRateLimiter a per worker rate limiter to throttle async persistence
   */
  public FileDataManager(BlockWorker blockWorker, UnderFileSystem ufs,
      RateLimiter persistenceRateLimiter) {
    mBlockWorker = Preconditions.checkNotNull(blockWorker);
    mPersistingInProgressFiles = new HashMap<>();
    mPersistedFiles = new HashSet<>();
    mUfs = ufs;
    mPersistenceRateLimiter = persistenceRateLimiter;
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
    String ufsRoot = Configuration.get(PropertyKey.UNDERFS_ADDRESS);
    FileInfo fileInfo = mBlockWorker.getFileInfo(fileId);
    String dstPath = PathUtils.concatPath(ufsRoot, fileInfo.getPath());

    return mUfs.isFile(dstPath);
  }

  /**
   * Locks all the blocks of a given file Id.
   *
   * @param fileId the id of the file
   * @param blockIds the ids of the file's blocks
   * @throws IOException when an I/O exception occurs
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
   * @throws AlluxioException if an unexpected Alluxio exception is thrown
   * @throws IOException if the file persistence fails
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
    Permission perm = new Permission(fileInfo.getOwner(), fileInfo.getGroup(),
        (short) fileInfo.getMode());
    OutputStream outputStream = mUfs.create(dstPath, CreateOptions.defaults().setPermission(perm));
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
   * @throws AlluxioException if an unexpected Alluxio exception is thrown
   * @throws IOException if the folder creation fails
   */
  private String prepareUfsFilePath(long fileId) throws AlluxioException, IOException {
    String ufsRoot = Configuration.get(PropertyKey.UNDERFS_ADDRESS);
    FileInfo fileInfo = mBlockWorker.getFileInfo(fileId);
    AlluxioURI uri = new AlluxioURI(fileInfo.getPath());
    AlluxioURI dstPath = new AlluxioURI(PathUtils.concatPath(ufsRoot, fileInfo.getPath()));
    LOG.info("persist file {} at {}", fileId, dstPath);
    String parentPath = PathUtils.concatPath(ufsRoot, uri.getParent().getPath());
    // creates the parent folder if it does not exist
    if (!mUfs.isDirectory(parentPath)) {
      FileSystem fs = FileSystem.Factory.get();
      // Create ancestor directories from top to the bottom. We cannot use recursive create parents
      // here because the permission for the ancestors can be different.
      Stack<Pair<String, MkdirsOptions>> ufsDirsToMakeWithOptions = new Stack<>();
      AlluxioURI curAlluxioPath = uri.getParent();
      AlluxioURI curUfsPath = dstPath.getParent();
      // Stop at the Alluxio root because the mapped directory of Alluxio root in UFS may not exist.
      while (!mUfs.isDirectory(curUfsPath.toString()) && curAlluxioPath != null) {
        URIStatus curDirStatus;
        curDirStatus = fs.getStatus(curAlluxioPath);
        Permission perm = new Permission(curDirStatus.getOwner(), curDirStatus.getGroup(),
            (short) curDirStatus.getMode());
        ufsDirsToMakeWithOptions.push(new Pair<>(curUfsPath.toString(),
            MkdirsOptions.defaults().setCreateParent(false).setPermission(perm)));
        curAlluxioPath = curAlluxioPath.getParent();
        curUfsPath = curUfsPath.getParent();
      }
      while (!ufsDirsToMakeWithOptions.empty()) {
        Pair<String, MkdirsOptions> ufsDirAndPerm = ufsDirsToMakeWithOptions.pop();
        // UFS mkdirs might fail if the directory is already created. If so, skip the mkdirs
        // and assume the directory is already prepared, regardless of permission matching.
        if (!mUfs.mkdirs(ufsDirAndPerm.getFirst(), ufsDirAndPerm.getSecond())
            && !mUfs.isDirectory(ufsDirAndPerm.getFirst())) {
          throw new IOException("Failed to create dir: " + ufsDirAndPerm.getFirst());
        }
      }
    }
    return dstPath.toString();
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
