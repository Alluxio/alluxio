/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.master;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectWriter;

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
  /**
   * Create a new InodeFile from an image JSON element
   * 
   * @param ele
   *          the image JSON element
   * @return the created inode file.
   * @throws IOException
   */
  static InodeFile loadImage(ImageElement ele) throws IOException {
    long creationTimeMs = ele.getLong("creationTimeMs");
    int fileId = ele.getInt("id");
    String fileName = ele.getString("name");
    int parentId = ele.getInt("parentId");

    long blockSizeByte = ele.getLong("blockSizeByte");
    long length = ele.getLong("length");
    boolean isComplete = ele.getBoolean("complete");
    boolean isPinned = ele.getBoolean("pin");
    boolean isCache = ele.getBoolean("cache");
    String ufsPath = ele.getString("ufsPath");
    int dependencyId = ele.getInt("depId");
    long lastModificationTimeMs = ele.getLong("lastModificationTimeMs");

    InodeFile inode = new InodeFile(fileName, fileId, parentId, blockSizeByte, creationTimeMs);

    try {
      inode.setLength(length);
    } catch (Exception e) {
      throw new IOException(e);
    }
    inode.setComplete(isComplete);
    inode.setPinned(isPinned);
    inode.setCache(isCache);
    inode.setUfsPath(ufsPath);
    inode.setDependencyId(dependencyId);
    inode.setLastModificationTimeMs(lastModificationTimeMs);
    return inode;
  }

  private final long BLOCK_SIZE_BYTE;
  private long mLength = 0;
  private boolean mIsComplete = false;
  private boolean mCache = false;
  private String mUfsPath = "";

  private List<BlockInfo> mBlocks = new ArrayList<BlockInfo>(3);

  private int mDependencyId;

  /**
   * Create a new InodeFile.
   * 
   * @param name
   *          The name of the file
   * @param id
   *          The id of the file
   * @param parentId
   *          The id of the parent of the file
   * @param blockSizeByte
   *          The block size of the file, in bytes
   * @param creationTimeMs
   *          The creation time of the file, in milliseconds
   */
  public InodeFile(String name, int id, int parentId, long blockSizeByte, long creationTimeMs) {
    super(name, id, parentId, false, creationTimeMs);
    BLOCK_SIZE_BYTE = blockSizeByte;
    mDependencyId = -1;
  }

  /**
   * Add a block to the file.It will check the legality. Cannot add the block if the file is
   * complete or the block's information doesn't match the file's information.
   * 
   * @param blockInfo
   *          The block to be added
   * @throws BlockInfoException
   */
  public synchronized void addBlock(BlockInfo blockInfo) throws BlockInfoException {
    if (mIsComplete) {
      throw new BlockInfoException("The file is complete: " + this);
    }
    if (mBlocks.size() > 0 && mBlocks.get(mBlocks.size() - 1).LENGTH != BLOCK_SIZE_BYTE) {
      throw new BlockInfoException("BLOCK_SIZE_BYTE is " + BLOCK_SIZE_BYTE + ", but the "
          + "previous block size is " + mBlocks.get(mBlocks.size() - 1).LENGTH);
    }
    if (blockInfo.getInodeFile() != this) {
      throw new BlockInfoException("InodeFile unmatch: " + this + " != " + blockInfo);
    }
    if (blockInfo.BLOCK_INDEX != mBlocks.size()) {
      throw new BlockInfoException("BLOCK_INDEX unmatch: " + mBlocks.size() + " != " + blockInfo);
    }
    if (blockInfo.OFFSET != mBlocks.size() * BLOCK_SIZE_BYTE) {
      throw new BlockInfoException("OFFSET unmatch: " + mBlocks.size() * BLOCK_SIZE_BYTE + " != "
          + blockInfo);
    }
    if (blockInfo.LENGTH > BLOCK_SIZE_BYTE) {
      throw new BlockInfoException("LENGTH too big: " + BLOCK_SIZE_BYTE + " " + blockInfo);
    }
    mLength += blockInfo.LENGTH;
    mBlocks.add(blockInfo);
  }

  /**
   * Add a location information of the file. A worker caches a block of the file.
   * 
   * @param blockIndex
   *          The index of the block in the file
   * @param workerId
   *          The id of the worker
   * @param workerAddress
   *          The net address of the worker
   * @throws BlockInfoException
   */
  public synchronized void addLocation(int blockIndex, long workerId, NetAddress workerAddress, long storageId)
      throws BlockInfoException {
    if (blockIndex < 0 || blockIndex >= mBlocks.size()) {
      throw new BlockInfoException("BlockIndex " + blockIndex + " out of bounds." + toString());
    }
    mBlocks.get(blockIndex).addLocation(workerId, workerAddress, storageId);
  }

  @Override
  public ClientFileInfo generateClientFileInfo(String path) {
    ClientFileInfo ret = new ClientFileInfo();

    ret.id = getId();
    ret.name = getName();
    ret.path = path;
    ret.ufsPath = mUfsPath;
    ret.length = mLength;
    ret.blockSizeByte = BLOCK_SIZE_BYTE;
    ret.creationTimeMs = getCreationTimeMs();
    ret.isComplete = isComplete();
    ret.isFolder = false;
    ret.isPinned = isPinned();
    ret.isCache = mCache;
    ret.blockIds = getBlockIds();
    ret.dependencyId = mDependencyId;
    ret.inMemoryPercentage = getInMemoryPercentage();
    ret.lastModificationTimeMs = getLastModificationTimeMs();

    return ret;
  }

  /**
   * Get the id of the specified block by the offset of the file.
   * 
   * @param offset
   *          The offset of the file
   * @return the id of the specified block
   */
  public long getBlockIdBasedOnOffset(long offset) {
    int index = (int) (offset / BLOCK_SIZE_BYTE);
    return BlockInfo.computeBlockId(getId(), index);
  }

  /**
   * Get all the blocks of the file. It will return a duplication.
   * 
   * @return a duplication of all the blocks' ids of the file
   */
  public synchronized List<Long> getBlockIds() {
    List<Long> ret = new ArrayList<Long>(mBlocks.size());
    for (int k = 0; k < mBlocks.size(); k ++) {
      ret.add(mBlocks.get(k).BLOCK_ID);
    }
    return ret;
  }

  /**
   * The pairs of the blocks and workers. Each pair contains a block's id and the id of the worker
   * who caches it.
   * 
   * @return all the pairs of the blocks and the workers
   */
  public synchronized List<Pair<Long, Long>> getBlockIdWorkerIdPairs() {
    List<Pair<Long, Long>> ret = new ArrayList<Pair<Long, Long>>();
    for (BlockInfo info : mBlocks) {
      ret.addAll(info.getBlockIdWorkerIdPairs());
    }
    return ret;
  }

  /**
   * Get the block list of the file, which is not a duplication.
   * 
   * @return the block list of the file
   */
  public List<BlockInfo> getBlockList() {
    return mBlocks;
  }

  /**
   * Get the locations of the specified block.
   * 
   * @param blockIndex
   *          The index of the block in the file
   * @return a list of the worker's net address who caches the block
   * @throws BlockInfoException
   */
  public synchronized List<NetAddress> getBlockLocations(int blockIndex) throws BlockInfoException {
    if (blockIndex < 0 || blockIndex > mBlocks.size()) {
      throw new BlockInfoException("BlockIndex is out of the boundry: " + blockIndex);
    }

    return mBlocks.get(blockIndex).getLocations();
  }

  /**
   * Get the block size of the file
   * 
   * @return the block size in bytes
   */
  public long getBlockSizeByte() {
    return BLOCK_SIZE_BYTE;
  }

  /**
   * Get the path of the file in under file system
   * 
   * @return the path of the file in under file system
   */
  public synchronized String getUfsPath() {
    return mUfsPath;
  }

  /**
   * Get a ClientBlockInfo of the specified block.
   * 
   * @param blockIndex
   *          The index of the block in the file
   * @return the generated ClientBlockInfo
   * @throws BlockInfoException
   */
  public synchronized ClientBlockInfo getClientBlockInfo(int blockIndex) throws BlockInfoException {
    if (blockIndex < 0 || blockIndex > mBlocks.size()) {
      throw new BlockInfoException("BlockIndex is out of the boundry: " + blockIndex);
    }

    return mBlocks.get(blockIndex).generateClientBlockInfo();
  }

  /**
   * Get file's all blocks' ClientBlockInfo information.
   * 
   * @return all blocks ClientBlockInfo
   */
  public synchronized List<ClientBlockInfo> getClientBlockInfos() {
    List<ClientBlockInfo> ret = new ArrayList<ClientBlockInfo>(mBlocks.size());
    for (BlockInfo tInfo : mBlocks) {
      ret.add(tInfo.generateClientBlockInfo());
    }
    return ret;
  }

  /**
   * Get the dependency id of the file
   * 
   * @return the dependency id of the file
   */
  public synchronized int getDependencyId() {
    return mDependencyId;
  }

  /**
   * Get the percentage that how many of the file is in memory.
   * 
   * @return the in memory percentage
   */
  private synchronized int getInMemoryPercentage() {
    if (mLength == 0) {
      return 100;
    }

    long inMemoryLength = 0;
    for (BlockInfo info : mBlocks) {
      if (info.isInMemory()) {
        inMemoryLength += info.LENGTH;
      }
    }
    return (int) (inMemoryLength * 100 / mLength);
  }

  /**
   * Get the length of the file
   * 
   * @return the length of the file
   */
  public synchronized long getLength() {
    return mLength;
  }

  /**
   * Get the id of a new block of the file. Also the id of the next block added into the file.
   * 
   * @return the id of a new block of the file
   */
  public synchronized long getNewBlockId() {
    return BlockInfo.computeBlockId(getId(), mBlocks.size());
  }

  /**
   * Get the number of the blocks of the file
   * 
   * @return the number of the blocks
   */
  public synchronized int getNumberOfBlocks() {
    return mBlocks.size();
  }

  /**
   * Return whether the file has checkpointed or not. Note that the file has checkpointed only if
   * the under file system path is not empty.
   * 
   * @return true if the file has checkpointed, false otherwise
   */
  public synchronized boolean hasCheckpointed() {
    return !mUfsPath.equals("");
  }

  /**
   * Return whether the file is cacheable or not.
   * 
   * @return true if the file is cacheable, false otherwise
   */
  public synchronized boolean isCache() {
    return mCache;
  }

  /**
   * Return whether the file is complete or not.
   * 
   * @return true if the file is complete, false otherwise
   */
  public synchronized boolean isComplete() {
    return mIsComplete;
  }

  /**
   * Return whether the file is fully in memory or not. The file is fully in memory only if all the
   * blocks of the file are in memory, in other words, the in memory percentage is 100.
   * 
   * @return true if the file is fully in memory, false otherwise
   */
  public synchronized boolean isFullyInMemory() {
    return getInMemoryPercentage() == 100;
  }

  /**
   * Remove a location of a block.
   * 
   * @param blockIndex
   *          The index of the block in the file
   * @param workerId
   *          The id of the removed location worker
   * @throws BlockInfoException
   */
  public synchronized void removeLocation(int blockIndex, long workerId) throws BlockInfoException {
    if (blockIndex < 0 || blockIndex >= mBlocks.size()) {
      throw new BlockInfoException("BlockIndex " + blockIndex + " out of bounds." + toString());
    }
    mBlocks.get(blockIndex).removeLocation(workerId);
  }

  /**
   * Set whether the file is cacheable or not.
   * 
   * @param cache
   *          If true, the file is cacheable
   */
  public synchronized void setCache(boolean cache) {
    // TODO this related logic is not complete right. fix this.
    mCache = cache;
  }

  /**
   * Set the path of the file in under file system.
   * 
   * @param ufsPath
   *          The new path of the file in under file system
   */
  public synchronized void setUfsPath(String ufsPath) {
    mUfsPath = ufsPath;
  }

  /**
   * The file is complete. Set the complete flag true.
   */
  public synchronized void setComplete() {
    mIsComplete = true;
  }

  /**
   * Set the complete flag of the file
   * 
   * @param complete
   *          If true, the file is complete
   */
  public synchronized void setComplete(boolean complete) {
    mIsComplete = complete;
  }

  /**
   * Set the dependency id of the file
   * 
   * @param dependencyId
   *          The new dependency id of the file
   */
  public synchronized void setDependencyId(int dependencyId) {
    mDependencyId = dependencyId;
  }

  /**
   * Set the length of the file. Cannot set the length if the file is complete or the length is
   * negative.
   * 
   * @param length
   *          The new length of the file, cannot be negative
   * @throws SuspectedFileSizeException
   * @throws BlockInfoException
   */
  public synchronized void setLength(long length) throws SuspectedFileSizeException,
      BlockInfoException {
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

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("InodeFile(");
    sb.append(super.toString()).append(", LENGTH: ").append(mLength);
    sb.append(", UfsPath: ").append(mUfsPath);
    sb.append(", mBlocks: ").append(mBlocks);
    sb.append(", DependencyId:").append(mDependencyId).append(")");
    return sb.toString();
  }

  @Override
  public synchronized void writeImage(ObjectWriter objWriter, DataOutputStream dos)
      throws IOException {
    ImageElement ele =
        new ImageElement(ImageElementType.InodeFile)
            .withParameter("creationTimeMs", getCreationTimeMs()).withParameter("id", getId())
            .withParameter("name", getName()).withParameter("parentId", getParentId())
            .withParameter("blockSizeByte", getBlockSizeByte())
            .withParameter("length", getLength()).withParameter("complete", isComplete())
            .withParameter("pin", isPinned()).withParameter("cache", isCache())
            .withParameter("ufsPath", getUfsPath()).withParameter("depId", getDependencyId())
            .withParameter("lastModificationTimeMs", getLastModificationTimeMs());

    writeElement(objWriter, dos, ele);
  }
}
