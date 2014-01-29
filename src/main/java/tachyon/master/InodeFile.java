/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.master;

import java.util.ArrayList;
import java.util.List;

import tachyon.Pair;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.ClientBlockInfo;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.NetAddress;
import tachyon.thrift.SuspectedFileSizeException;

/**
 * Tachyon file system's file representation in master.
 */
public class InodeFile extends Inode {
  private final long BLOCK_SIZE_BYTE;

  private long mLength = 0;
  private boolean mIsComplete = false;
  private boolean mPin = false;
  private boolean mCache = false;
  private String mCheckpointPath = "";
  private List<BlockInfo> mBlocks = new ArrayList<BlockInfo>(3);

  private int mDependencyId;

  public InodeFile(String name, int id, int parentId, long blockSizeByte, long creationTimeMs) {
    super(name, id, parentId, InodeType.File, creationTimeMs);
    BLOCK_SIZE_BYTE = blockSizeByte;
    mDependencyId = -1;
  }

  public synchronized long getLength() {
    return mLength;
  }

  public synchronized void setLength(long length) 
      throws SuspectedFileSizeException, BlockInfoException {
    if (isComplete()) {
      throw new SuspectedFileSizeException("InodeFile length was set previously.");
    }
    if (length < 0) {
      throw new SuspectedFileSizeException("InodeFile new length " + length + " is illegal.");
    }
    mLength = 0;
    while (length >= BLOCK_SIZE_BYTE) {
      addBlock(new BlockInfo(this, mBlocks.size(), BLOCK_SIZE_BYTE));
      length -= BLOCK_SIZE_BYTE;
    }
    if (length > 0) {
      addBlock(new BlockInfo(this, mBlocks.size(), (int) length));
    }
    mIsComplete = true;
  }

  public synchronized boolean isComplete() {
    return mIsComplete;
  }

  public synchronized void setComplete() {
    mIsComplete = true;
  }

  public synchronized void setComplete(boolean complete) {
    mIsComplete = complete;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("InodeFile(");
    sb.append(super.toString()).append(", LENGTH: ").append(mLength);
    sb.append(", CheckpointPath: ").append(mCheckpointPath);
    sb.append(", mBlocks: ").append(mBlocks);
    sb.append(", DependencyId:").append(mDependencyId).append(")");
    return sb.toString();
  }

  public synchronized void setCheckpointPath(String checkpointPath) {
    mCheckpointPath = checkpointPath;
  }

  public synchronized String getCheckpointPath() {
    return mCheckpointPath;
  }

  public synchronized long getNewBlockId() {
    return BlockInfo.computeBlockId(getId(), mBlocks.size());
  }

  public synchronized void addBlock(BlockInfo blockInfo) throws BlockInfoException {
    if (mIsComplete) {
      throw new BlockInfoException("The file is complete: " + this);
    }
    if (mBlocks.size() > 0 && mBlocks.get(mBlocks.size() - 1).LENGTH != BLOCK_SIZE_BYTE) {
      throw new BlockInfoException("BLOCK_SIZE_BYTE is " + BLOCK_SIZE_BYTE + ", but the " +
          "previous block size is " + mBlocks.get(mBlocks.size() - 1).LENGTH);
    }
    if (blockInfo.getInodeFile() != this) {
      throw new BlockInfoException("InodeFile unmatch: " + this + " != " + blockInfo);
    }
    if (blockInfo.BLOCK_INDEX != mBlocks.size()) {
      throw new BlockInfoException("BLOCK_INDEX unmatch: " + mBlocks.size() + " != " + blockInfo);
    }
    if (blockInfo.OFFSET != (long) mBlocks.size() * BLOCK_SIZE_BYTE) {
      throw new BlockInfoException("OFFSET unmatch: " + (long) mBlocks.size() * BLOCK_SIZE_BYTE +
          " != " + blockInfo);
    }
    if (blockInfo.LENGTH > BLOCK_SIZE_BYTE) {
      throw new BlockInfoException("LENGTH too big: " + BLOCK_SIZE_BYTE + " " + blockInfo);
    }
    mLength += blockInfo.LENGTH;
    mBlocks.add(blockInfo);
  }

  public synchronized void addLocation(int blockIndex, long workerId, NetAddress workerAddress) 
      throws BlockInfoException {
    if (blockIndex < 0 || blockIndex >= mBlocks.size()) {
      throw new BlockInfoException("BlockIndex " + blockIndex + " out of bounds." + toString());
    }
    mBlocks.get(blockIndex).addLocation(workerId, workerAddress);
  }

  public synchronized void removeLocation(int blockIndex, long workerId) throws BlockInfoException {
    if (blockIndex < 0 || blockIndex >= mBlocks.size()) {
      throw new BlockInfoException("BlockIndex " + blockIndex + " out of bounds." + toString());
    }
    mBlocks.get(blockIndex).removeLocation(workerId);
  }

  public long getBlockIdBasedOnOffset(long offset) {
    int index = (int) (offset / BLOCK_SIZE_BYTE);
    return BlockInfo.computeBlockId(getId(), index);
  }

  public long getBlockSizeByte() {
    return BLOCK_SIZE_BYTE;
  }

  public synchronized List<NetAddress> getBlockLocations(int blockIndex) throws BlockInfoException {
    if (blockIndex < 0 || blockIndex > mBlocks.size()) {
      throw new BlockInfoException("BlockIndex is out of the boundry: " + blockIndex);
    }

    return mBlocks.get(blockIndex).getLocations();
  }

  public synchronized ClientBlockInfo getClientBlockInfo(int blockIndex) throws BlockInfoException {
    if (blockIndex < 0 || blockIndex > mBlocks.size()) {
      throw new BlockInfoException("BlockIndex is out of the boundry: " + blockIndex);
    }

    return mBlocks.get(blockIndex).generateClientBlockInfo();
  }

  /**
   * Get file's all blocks' ClientBlockInfo information.
   * @return all blocks ClientBlockInfo
   */
  public synchronized List<ClientBlockInfo> getClientBlockInfos() {
    List<ClientBlockInfo> ret = new ArrayList<ClientBlockInfo>(mBlocks.size());
    for (BlockInfo tInfo: mBlocks) {
      ret.add(tInfo.generateClientBlockInfo());
    }
    return ret;
  }

  public synchronized boolean isFullyInMemory() {
    return getInMemoryPercentage() == 100;
  }

  private synchronized int getInMemoryPercentage() {
    if (mLength == 0) {
      return 100;
    }

    long inMemoryLength = 0;
    for (BlockInfo info: mBlocks) {
      if (info.isInMemory()) {
        inMemoryLength += info.LENGTH;
      }
    }
    return (int) (inMemoryLength * 100 / mLength);
  }

  public synchronized void setPin(boolean pin) {
    mPin = pin;
  }

  public synchronized boolean isPin() {
    return mPin;
  }

  public synchronized void setCache(boolean cache) {
    mCache = cache;
  }

  public synchronized boolean isCache() {
    return mCache;
  }

  public synchronized boolean hasCheckpointed() {
    return !mCheckpointPath.equals("");
  }

  public synchronized void setDependencyId(int dependencyId) {
    mDependencyId = dependencyId;
  }

  public synchronized int getDependencyId() {
    return mDependencyId;
  }

  public synchronized List<Long> getBlockIds() {
    List<Long> ret = new ArrayList<Long>(mBlocks.size());
    for (int k = 0; k < mBlocks.size(); k ++) {
      ret.add(mBlocks.get(k).BLOCK_ID);
    }
    return ret;
  }

  public synchronized int getNumberOfBlocks() {
    return mBlocks.size();
  }

  public synchronized List<Pair<Long, Long>> getBlockIdWorkerIdPairs() {
    List<Pair<Long, Long>> ret = new ArrayList<Pair<Long, Long>>();
    for (BlockInfo info: mBlocks) {
      ret.addAll(info.getBlockIdWorkerIdPairs());
    }
    return ret;
  }

  @Override
  public ClientFileInfo generateClientFileInfo(String path) {
    ClientFileInfo ret = new ClientFileInfo();

    ret.id = getId();
    ret.name = getName();
    ret.path = path;
    ret.checkpointPath = mCheckpointPath;
    ret.length = mLength;
    ret.blockSizeByte = BLOCK_SIZE_BYTE;
    ret.creationTimeMs = getCreationTimeMs();
    ret.complete = isComplete();
    ret.folder = false;
    ret.inMemory = isFullyInMemory();
    ret.needPin = mPin;
    ret.needCache = mCache;
    ret.blockIds = getBlockIds();
    ret.dependencyId = mDependencyId;

    return ret;
  }
}