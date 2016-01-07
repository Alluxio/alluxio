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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

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
import tachyon.worker.block.BlockDataManager;
import tachyon.worker.block.io.BlockReader;

/**
 * Responsible for Storing files into under file system.
 */
public final class FileDataManager {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final UnderFileSystem mUfs;
  /** Block data manager for access block info */
  private final BlockDataManager mBlockDataManager;
  private final List<Long> mPersistedFiles;
  private final TachyonConf mTachyonConf;

  /**
   * Creates a new instance of {@link FileDataManager}.
   *
   * @param blockDataManager a block data manager handle
   */
  public FileDataManager(BlockDataManager blockDataManager) {
    mBlockDataManager = Preconditions.checkNotNull(blockDataManager);
    mPersistedFiles = Lists.newArrayList();
    mTachyonConf = WorkerContext.getConf();
    // Create Under FileSystem Client
    String ufsAddress = mTachyonConf.get(Constants.UNDERFS_ADDRESS);
    mUfs = UnderFileSystem.get(ufsAddress, mTachyonConf);
  }

  /**
   * Persists the blocks of a file into the under file system.
   *
   * @param fileId the id of the file
   * @param blockIds the list of block ids
   * @throws IOException if the file persistence fails
   */
  public synchronized void persistFile(long fileId, List<Long> blockIds) throws IOException {
    String dstPath = prepareUfsFilePath(fileId);
    OutputStream outputStream = mUfs.create(dstPath);
    final WritableByteChannel outputChannel = Channels.newChannel(outputStream);

    Map<Long, Long> blockIdToLockId = Maps.newHashMap();
    List<Throwable> errors = new ArrayList<Throwable>();
    try {
      // lock all the blocks to prevent any eviction
      for (long blockId : blockIds) {
        long lockId = mBlockDataManager.lockBlock(Sessions.CHECKPOINT_SESSION_ID, blockId);
        blockIdToLockId.put(blockId, lockId);
      }

      for (long blockId : blockIds) {
        long lockId = blockIdToLockId.get(blockId);

        // obtain block reader
        BlockReader reader =
            mBlockDataManager.readBlockRemote(Sessions.CHECKPOINT_SESSION_ID, blockId, lockId);

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
          mBlockDataManager.unlockBlock(lockId);
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
    mPersistedFiles.add(fileId);
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
    FileInfo fileInfo = mBlockDataManager.getFileInfo(fileId);
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
   * Returns a list of file to persist, removing them from the internal queue.
   *
   * @return a list to files to persist
   */
  public synchronized List<Long> popPersistedFiles() {
    List<Long> toReturn = Lists.newArrayList();
    toReturn.addAll(mPersistedFiles);
    mPersistedFiles.clear();
    return toReturn;
  }
}
