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

package tachyon.worker.block;

import com.google.common.base.Optional;

import tachyon.Users;
import tachyon.conf.TachyonConf;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.OutOfSpaceException;
import tachyon.worker.BlockStoreLocation;
import tachyon.worker.block.io.BlockReader;
import tachyon.worker.block.io.BlockWriter;
import tachyon.worker.block.meta.BlockMeta;
import tachyon.worker.block.meta.TempBlockMeta;

import java.io.IOException;
import java.util.List;

/**
 * Class responsible for managing the Tachyon BlockStore and Under FileSystem. This class provides
 * thread safety.
 */
public class BlockDataManager {
  private final BlockStore mBlockStore;
  private final TachyonConf mTachyonConf;

  private Users mUsers;

  public BlockDataManager(TachyonConf tachyonConf) {
    mBlockStore = new TieredBlockStore();
    mTachyonConf = tachyonConf;
  }

  public boolean abortBlock(long userId, long blockId) throws IOException {
    return mBlockStore.abortBlock(userId, blockId);
  }

  public void accessBlock(long userId, long blockId) {
    mBlockStore.accessBlock(userId, blockId);
  }

  public void cleanupUsers() {
    for (long user : mUsers.getTimedOutUsers()) {
      mUsers.removeUser(user);
      mBlockStore.cleanupUser(user);
    }
  }

  public boolean commitBlock(long userId, long blockId) throws IOException {
    return mBlockStore.commitBlock(userId, blockId);
  }

  public String createBlock(long userId, long blockId, int location, long initialBytes)
      throws IOException, OutOfSpaceException {
    BlockStoreLocation loc = BlockStoreLocation.anyDirInTier(location);
    Optional<TempBlockMeta> optBlock =
        mBlockStore.createBlockMeta(userId, blockId, loc, initialBytes);
    if (optBlock.isPresent()) {
      return optBlock.get().getPath();
    }
    // Failed to allocate initial bytes
    throw new OutOfSpaceException("Failed to allocate " + initialBytes + " for user " + userId);
  }

  public BlockWriter createBlockRemote(long userId, long blockId, int location, long initialBytes)
      throws FileDoesNotExistException, IOException {
    BlockStoreLocation loc = BlockStoreLocation.anyDirInTier(location);
    Optional<TempBlockMeta> optBlock =
        mBlockStore.createBlockMeta(userId, blockId, loc, initialBytes);
    if (optBlock.isPresent()) {
      Optional<BlockWriter> optWriter = mBlockStore.getBlockWriter(userId, blockId);
      if (optWriter.isPresent()) {
        return optWriter.get();
      }
      // TODO: Throw a better exception
      throw new IOException("Failed to obtain block writer.");
    }
    throw new FileDoesNotExistException("Block " + blockId + " does not exist on this worker.");
  }

  public boolean freeBlock(long userId, long blockId) throws IOException {
    Optional<Long> optLock = mBlockStore.lockBlock(userId, blockId, BlockLock.BlockLockType.WRITE);
    if (!optLock.isPresent()) {
      return false;
    }
    Long lockId = optLock.get();
    mBlockStore.removeBlock(userId, blockId, lockId);
    mBlockStore.unlockBlock(lockId);
    return true;
  }

  // TODO: Implement this
  public BlockHeartbeatReport getReport() {
    return null;
  }

  public StoreMeta getStoreMeta() {
    return mBlockStore.getStoreMeta();
  }

  public String getUserUfsTmpFolder(long userId) {
    return mUsers.getUserUfsTempFolder(userId);
  }

  public long lockBlock(long userId, long blockId, int type) {
    // TODO: Define some conversion of int -> lock type
    Optional<Long> optLock = mBlockStore.lockBlock(userId, blockId, BlockLock.BlockLockType.WRITE);
    if (optLock.isPresent()) {
      return optLock.get();
    }
    // TODO: Decide on failure return value
    return -1;
  }

  public String readBlock(long userId, long blockId, long lockId) throws FileDoesNotExistException {
    Optional<BlockMeta> optBlock = mBlockStore.getBlockMeta(userId, blockId, lockId);
    if (optBlock.isPresent()) {
      return optBlock.get().getPath();
    }
    // Failed to find the block
    throw new FileDoesNotExistException("Block " + blockId + " does not exist on this worker.");
  }

  public BlockReader readBlockRemote(long userId, long blockId, long lockId)
      throws FileDoesNotExistException, IOException {
    Optional<BlockReader> optReader = mBlockStore.getBlockReader(userId, blockId, lockId);
    if (optReader.isPresent()) {
      return optReader.get();
    }
    throw new FileDoesNotExistException("Block " + blockId + " does not exist on this worker.");
  }

  public boolean moveBlock(long userId, long blockId, int tier) throws IOException {
    Optional<Long> optLock = mBlockStore.lockBlock(userId, blockId, BlockLock.BlockLockType.WRITE);
    // TODO: Define this behavior
    if (!optLock.isPresent()) {
      return false;
    }
    Long lockId = optLock.get();
    BlockStoreLocation dst = BlockStoreLocation.anyDirInTier(tier);
    boolean result = mBlockStore.moveBlock(userId, blockId, lockId, dst);
    mBlockStore.unlockBlock(lockId);
    return result;
  }

  public boolean requestSpace(long userId, long blockId, long bytesRequested) throws IOException {
    return mBlockStore.requestSpace(userId, blockId, bytesRequested);
  }

  public void setUsers(Users users) {
    mUsers = users;
  }

  public boolean unlockBlock(long lockId) {
    return mBlockStore.unlockBlock(lockId);
  }

  public boolean userHeartbeat(long userId, List<Long> metrics) {
    mUsers.userHeartbeat(userId);
    return true;
  }
}
