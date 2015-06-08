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

package tachyon.worker;

import com.google.common.base.Optional;
import tachyon.conf.TachyonConf;
import tachyon.thrift.OutOfSpaceException;
import tachyon.worker.block.BlockFileOperator;
import tachyon.worker.block.BlockLock;
import tachyon.worker.block.BlockStore;
import tachyon.worker.block.TieredBlockStore;
import tachyon.worker.block.meta.BlockMeta;

import java.io.IOException;

public class CoreWorker {
  private final BlockStore mBlockStore;
  private final TachyonConf mTachyonConf;

  public CoreWorker(TachyonConf tachyonConf) {
    mBlockStore = new TieredBlockStore();
    mTachyonConf = tachyonConf;
  }

  public boolean cancelBlock(int userId, long blockId) {
    return mBlockStore.abortBlock(userId, blockId);
  }

  public String createBlock(int userId, long blockId, int location, long initialBytes)
      throws OutOfSpaceException {
    BlockStoreLocation loc = new BlockStoreLocation(location);
    Optional<BlockMeta> optBlock = mBlockStore.createBlockMeta(userId, blockId, loc, initialBytes);
    if (optBlock.isPresent()) {
      return optBlock.get().getTmpPath();
    }
    // Failed to allocate initial bytes
    throw new OutOfSpaceException("Failed to allocate " + initialBytes + " for user " + userId);
  }

  public BlockWriter createBlockRemote(int userId, long blockId, int location,
      long initialBytes) throws IOException {
    BlockStoreLocation loc = new BlockStoreLocation(location);
    Optional<BlockMeta> optBlock = mBlockStore.createBlockMeta(userId, blockId, loc, initialBytes);
    if (optBlock.isPresent()) {
      Optional<BlockWriter> optWriter = mBlockStore.getBlockWriter(userId, blockId);
      if (optWriter.isPresent()) {
        return optWriter.get();
      }
      // TODO: Throw a better exception
      throw new IOException("Failed to obtain block writer");
    }
    // TODO: Throw a better exception
    throw new IOException("Block " + blockId + " does not exist on this worker.");
  }

  // TODO: Implement this
  public String getUserUfsTmpFolder(int userId) {
    return null;
  }

  public boolean persistBlock(int userId, long blockId) {
    return mBlockStore.commitBlock(userId, blockId);
  }

  public String readBlock(int userId, long blockId, int lockId) throws IOException {
    Optional<BlockMeta> optBlock = mBlockStore.getBlockMeata(userId, blockId, lockId);
    if (optBlock.isPresent()) {
      return optBlock.get().getPath();
    }
    // Failed to find the block
    // TODO: Throw a better exception
    throw new IOException("Block " + blockId + " does not exist on this worker.");
  }

  public BlockReader readBlockRemote(int userId, long blockId, int lockId) throws IOException {
    Optional<BlockReader> optReader = mBlockStore.getBlockReader(userId, blockId, lockId);
    if (optReader.isPresent()) {
      return optReader.get();
    }
    // TODO: Throw a better exception
    throw new IOException("Block " + blockId + " does not exist on this worker.");
  }

  public boolean relocateBlock(int userId, long blockId, int destination) {
    Optional<Long> optLock = mBlockStore.lockBlock(userId, blockId, BlockLock.BlockLockType.WRITE);
    // TODO: Define this behavior
    if (!optLock.isPresent()) {
      return false;
    }
    Long lockId = optLock.get();
    Optional<BlockMeta> optMeta = mBlockStore.getBlockMeata(userId, blockId, lockId);
    // TODO: Define this behavior
    if (!optMeta.isPresent()) {
      // TODO: what if this fails?
      mBlockStore.unlockBlock(lockId);
      return false;
    }
    // TODO: Add this to the BlockMeta API
    BlockStoreLocation oldLoc = optMeta.get().getLocation();
    BlockStoreLocation newLoc = new BlockStoreLocation(destination);
    if (mBlockStore.copyBlock(userId, blockId, lockId, newLoc)) {
      // TODO: What if this fails?
      mBlockStore.removeBlock(userId, blockId, lockId, oldLoc);
      mBlockStore.unlockBlock(lockId);
      return true;
    } else {
      mBlockStore.unlockBlock(lockId);
      return false;
    }
  }

  public boolean requestSpace(int userId, long blockId, long bytesRequested) {
    return mBlockStore.requestSpace(userId, blockId, bytesRequested);
  }

  public long lockBlock(int userId, long blockId, int type) {
    // TODO: Define some conversion of int -> lock type
    Optional<Long> optLock = mBlockStore.lockBlock(userId, blockId, BlockLock.BlockLockType.WRITE);
    if (optLock.isPresent()) {
      return optLock.get();
    }
    // TODO: Decide on failure return value
    return -1;
  }

  public boolean userHeartbeat(int userId) {
    // TODO: Update user metadata
    return true;
  }
}
