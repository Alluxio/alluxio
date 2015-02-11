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

package tachyon.master;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import tachyon.Pair;
import tachyon.StorageDirId;
import tachyon.StorageLevelAlias;
import tachyon.UnderFileSystem;
import tachyon.thrift.ClientBlockInfo;
import tachyon.thrift.NetAddress;
import tachyon.thrift.PageLocation;
import tachyon.thrift.WorkerInfo;
import tachyon.util.NetworkUtils;
import tachyon.util.PageUtils;

/**
 * Block info on the master side.
 */
public class BlockInfo {
  /**
   * Compute the block's id with the inode's id and the block's index in the inode. In Tachyon, the
   * blockId is equal to ((inodeId << 30) + blockIndex).
   * 
   * @param inodeId The inode's id of the block
   * @param blockIndex The block's index of the block in the inode
   * @return the block's id
   */
  public static long computeBlockId(int inodeId, int blockIndex) {
    return ((long) inodeId << 30) + blockIndex;
  }

  /**
   * Compute the block's index in the inode with the block's id. The blockIndex is the last 30 bits
   * of the blockId.
   * 
   * @param blockId The id of the block
   * @return the block's index in the inode
   */
  public static int computeBlockIndex(long blockId) {
    return (int) (blockId & 0x3fffffff);
  }

  /**
   * Compute the inode's id of the block. The inodeId is the first 34 bits of the blockId.
   * 
   * @param blockId The id of the block
   * @return the inode's id of the block
   */
  public static int computeInodeId(long blockId) {
    return (int) (blockId >> 30);
  }

  private final InodeFile mInodeFile;

  public final int mBlockIndex;
  public final long mBlockId;
  public final long mOffset;
  public final long mLength;

  /**
   * Maps workerIds to the worker address they refer to, for workers that have cached parts of this
   * block
   */
  private final Map<Long, NetAddress> mLocations = new HashMap<Long, NetAddress>(5);
  /**
   * Maps workerIds to the pageId and storageId they have cached. Since each page can only be cached
   * in one storage dir per worker, for each worker, we store a map of pageIds to their associated
   * storageId.
   */
  private final Map<Long, Map<Long, Long>> mCachedPages = new HashMap<Long, Map<Long, Long>>();

  /**
   * @param inodeFile
   * @param blockIndex
   * @param length Can not be no bigger than 2^31 - 1
   */
  BlockInfo(InodeFile inodeFile, int blockIndex, long length) {
    mInodeFile = inodeFile;
    mBlockIndex = blockIndex;
    mBlockId = computeBlockId(mInodeFile.getId(), mBlockIndex);
    mOffset = inodeFile.getBlockSizeByte() * blockIndex;
    mLength = length;
  }

  /**
   * Add a location of a given list of pages. It means that the worker has the data of those pages
   * in memory.
   * 
   * @param workerId The id of the worker
   * @param workerAddress The net address of the worker
   * @param storageDirId The id of the StorageDir which block is located in
   * @param pages A list of pageIds that are stored on the given worker in the given storageId
   */
  public synchronized void addLocation(long workerId, NetAddress workerAddress, long storageDirId,
      List<Long> pages) {
    mLocations.put(workerId, workerAddress);
    Map<Long, Long> existingPages = mCachedPages.get(workerId);
    if (existingPages == null) {
      existingPages = new HashMap<Long, Long>();
      mCachedPages.put(workerId, existingPages);
    }
    // This will overwrite any existing storageDir for pages that are already in the map. This is
    // intentional, since when we evict a page to a new storageDir, it is only reported as adding
    // the page to the new storageDir 
    for (Long page : pages) {
      existingPages.put(page, storageDirId);
    }
  }

  /**
   * Generate a ClientBlockInfo of the block, which is used for the thrift server.
   * 
   * @return the generated ClientBlockInfo
   */
  public synchronized ClientBlockInfo generateClientBlockInfo() {
    ClientBlockInfo ret = new ClientBlockInfo();

    ret.blockId = mBlockId;
    ret.offset = mOffset;
    ret.length = mLength;
    ret.workers = new ArrayList<WorkerInfo>();
    for (Map.Entry<Long, NetAddress> entry : mLocations.entrySet()) {
      List<PageLocation> pages = new ArrayList<PageLocation>();
      for (Map.Entry<Long, Long> pageEntry : mCachedPages.get(entry.getKey()).entrySet()) {
        pages.add(new PageLocation(pageEntry.getKey(), pageEntry.getValue()));
      }
      ret.workers.add(new WorkerInfo(entry.getValue(), pages));
    }
    ret.checkpoints = getCheckpoints();
    return ret;
  }

  /**
   * Get the list of pairs "blockId, workerId", where the blockId is the id of this block, and the
   * workerId is the id of the worker who has the block's data in memory.
   * 
   * @return the list of those pairs
   */
  public synchronized List<Pair<Long, Long>> getBlockIdWorkerIdPairs() {
    List<Pair<Long, Long>> ret = new ArrayList<Pair<Long, Long>>(mLocations.size());
    for (long workerId : mLocations.keySet()) {
      ret.add(new Pair<Long, Long>(mBlockId, workerId));
    }
    return ret;
  }

  /**
   * Get the InodeFile of the block
   * 
   * @return the InodeFile of the block
   */
  public synchronized InodeFile getInodeFile() {
    return mInodeFile;
  }

  /**
   * Get the addresses where the block is checkpointed
   * 
   * @return the addresses of the checkpoint locations
   */
  public synchronized List<String> getCheckpoints() {
    List<String> ret = new ArrayList<String>();
    if (mInodeFile.hasCheckpointed()) {
      UnderFileSystem ufs = UnderFileSystem.get(mInodeFile.getUfsPath());
      List<String> locs = null;
      try {
        locs = ufs.getFileLocations(mInodeFile.getUfsPath(), mOffset);
      } catch (IOException e) {
        return ret;
      }
      if (locs != null) {
        for (String loc : locs) {
          String resolvedHost;
          try {
            resolvedHost = NetworkUtils.resolveHostName(loc);
          } catch (UnknownHostException e) {
            resolvedHost = loc;
          }
          ret.add(resolvedHost);
        }
      }
    }
    return ret;
  }

  /**
   * Get the addresses of any worker that has part of the block in memory
   * 
   * @return the addresses of the locations
   */
  public synchronized List<NetAddress> getWorkerAddresses() {
    List<NetAddress> ret = new ArrayList<NetAddress>();
    ret.addAll(mLocations.values());
    return ret;
  }

  /**
   * @return true if the block is in some worker's memory, false otherwise
   */
  public synchronized boolean isInMemory() {
    // Every page must be cached in memory, so we first create a set of the pages that are cached in
    // memory
    Set<Long> cachedPages = new HashSet<Long>();
    for (Map<Long, Long> pages : mCachedPages.values()) {
      for (Map.Entry<Long, Long> entry : pages.entrySet()) {
        if (StorageDirId.getStorageLevelAliasValue(entry.getValue()) == StorageLevelAlias.MEM
            .getValue()) {
          cachedPages.add(entry.getKey());
        }
      }
    }
    // We only return true if the size of the pages set equals the number of pages in the block
    return cachedPages.size() == PageUtils.getNumPages(mLength);
  }

  /**
   * Remove the worker from the block's locations
   * 
   * @param workerId The id of the removed worker
   */
  public synchronized void removeLocation(long workerId) {
    mLocations.remove(workerId);
    mCachedPages.remove(workerId);
  }

  @Override
  public synchronized String toString() {
    StringBuilder sb = new StringBuilder("BlockInfo(mBlockIndex: ");
    sb.append(mBlockIndex);
    sb.append(", mBlockId: ").append(mBlockId);
    sb.append(", mOffset: ").append(mOffset);
    sb.append(", mLength: ").append(mLength);
    sb.append(", mLocations: ").append(mLocations);
    sb.append(", mCachedPages: ").append(mCachedPages).append(")");
    return sb.toString();
  }
}
