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

import com.google.common.base.Preconditions;

import tachyon.Pair;
import tachyon.StorageDirId;
import tachyon.StorageLevelAlias;
import tachyon.conf.TachyonConf;
import tachyon.underfs.UnderFileSystem;
import tachyon.thrift.ClientBlockInfo;
import tachyon.thrift.LocationInfo;
import tachyon.thrift.NetAddress;
import tachyon.thrift.LocationInfo;
import tachyon.util.NetworkUtils;

/**
 * Block info on the master side.
 */
public class BlockInfo {
  /**
   * Compute the block id based on its inode id and its index among all blocks of the inode. In
   * Tachyon, blockId is calculated by ((inodeId << 30) + blockIndex).
   *
   * @param inodeId The inode id of the block
   * @param blockIndex The index of the block in the inode
   * @return the block id
   */
  public static long computeBlockId(int inodeId, int blockIndex) {
    return ((long) inodeId << 30) + blockIndex;
  }

  /**
   * Compute the index of a block within its inode based on the block id. The blockIndex is the 30
   * least significant bits (LSBs) of the blockId.
   *
   * @param blockId The id of the block
   * @return the index of the block in the inode
   */
  public static int computeBlockIndex(long blockId) {
    return (int) (blockId & 0x3fffffff);
  }

  /**
   * Compute the inode id of the block. The inodeId is the 34 most significant bits (MSBs) of the
   * blockId.
   *
   * @param blockId The id of the block
   * @return the inode id of the block
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
   * Maps workerIds to the worker address they refer to, for workers that have cached this block
   */
  private final Map<Long, NetAddress> mLocations = new HashMap<Long, NetAddress>(5);

  /**
   * Maps workerIds to the storageIds they have pages cached in.
   */
  private final Map<Long, Set<Long>> mWorkerDirs = new HashMap<Long, Set<Long>>();

  /**
   * @param inodeFile
   * @param blockIndex
   * @param length must be smaller than 2^31 (i.e., 2GB)
   */
  BlockInfo(InodeFile inodeFile, int blockIndex, long length) {
    Preconditions.checkArgument((length >> 31) == 0, "length must be smaller than 2^31");
    mInodeFile = inodeFile;
    mBlockIndex = blockIndex;
    mBlockId = computeBlockId(mInodeFile.getId(), mBlockIndex);
    mOffset = inodeFile.getBlockSizeByte() * blockIndex;
    mLength = length;
  }

  /**
   * Add a location of the block.
   *
   * @param workerId The id of the worker
   * @param workerAddress The net address of the worker
   * @param storageDirId The id of the StorageDir which block is located in
   */
  public synchronized void addLocation(long workerId, NetAddress workerAddress, long storageDirId) {
    mLocations.put(workerId, workerAddress);
    Set<Long> existingDirs = mWorkerDirs.get(workerId);
    if (existingDirs == null) {
      existingDirs = new HashSet<Long>();
      mWorkerDirs.put(workerId, existingDirs);
    }
    existingDirs.add(storageDirId);
  }

  /**
   * Generate a ClientBlockInfo of the block, which is used for the thrift server.
   *
   * @return the generated ClientBlockInfo
   */
  public synchronized ClientBlockInfo generateClientBlockInfo(TachyonConf tachyonConf) {
    ClientBlockInfo ret = new ClientBlockInfo();

    ret.blockId = mBlockId;
    ret.offset = mOffset;
    ret.length = mLength;
    ret.workers = new ArrayList<LocationInfo>();
    for (Map.Entry<Long, NetAddress> entry : mLocations.entrySet()) {
      List<Long> storageDirIds = new ArrayList<Long>();
      storageDirIds.addAll(mWorkerDirs.get(entry.getKey()));
      ret.workers.add(new LocationInfo(entry.getValue(), storageDirIds));
    }
    ret.checkpoints = getCheckpoints(tachyonConf);
    return ret;
  }

  /**
   * Get the hostnames where the block is checkpointed.
   *
   * @return the hostnames of the checkpoints
   */
  public synchronized List<String> getCheckpoints(TachyonConf tachyonConf) {
    List<String> ret = new ArrayList<String>();
    if (mInodeFile.hasCheckpointed()) {
      UnderFileSystem ufs = UnderFileSystem.get(mInodeFile.getUfsPath(), tachyonConf);
      List<String> locs = null;
      try {
        locs = ufs.getFileLocations(mInodeFile.getUfsPath(), mOffset);
      } catch (IOException e) {
        return ret;
      }
      if (locs != null) {
        for (String loc : locs) {
          String resolvedHost = loc;
          int resolvedPort = -1;
          try {
            String[] ipport = loc.split(":");
            if (ipport.length == 2) {
              resolvedHost = NetworkUtils.resolveHostName(ipport[0]);
              resolvedPort = Integer.parseInt(ipport[1]);
            }
          } catch (UnknownHostException uhe) {
            continue;
          } catch (NumberFormatException nfe) {
            continue;
          }
          ret.add(resolvedHost);
        }
      }
    }
    return ret;
  }

  public synchronized List<NetAddress> getWorkerAddresses() {
    List<NetAddress> ret = new ArrayList<NetAddress>();
    ret.addAll(mLocations.values());
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
   * @return true if the block is in some worker's memory, false otherwise
   */
  public synchronized boolean isInMemory() {
    for (Set<Long> storageDirIdSet : mWorkerDirs.values()) {
      for (long storageDirId : storageDirIdSet) {
        if (StorageDirId.getStorageLevelAliasValue(storageDirId) == StorageLevelAlias.MEM
            .getValue()) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Remove the worker from the block's locations
   *
   * @param workerId The id of the removed worker
   * @param storageDirId the storage directory to remove
   */
  public synchronized void removeLocation(long workerId, long storageDirId) {
    Set<Long> dirSet = mWorkerDirs.get(workerId);
    if (dirSet != null) {
      dirSet.remove(storageDirId);
      if (dirSet.isEmpty()) {
        mWorkerDirs.remove(workerId);
        mLocations.remove(workerId);
      }
    }
  }

  /**
   * Remove all storage directories associated with the given worker
   *
   * @param workerId The id of the worker to remove
   */
  public synchronized void removeLocationEntirely(long workerId) {
    mWorkerDirs.remove(workerId);
    mLocations.remove(workerId);
  }

  @Override
  public synchronized String toString() {
    StringBuilder sb = new StringBuilder("BlockInfo(mBlockIndex: ");
    sb.append(mBlockIndex);
    sb.append(", mBlockId: ").append(mBlockId);
    sb.append(", mOffset: ").append(mOffset);
    sb.append(", mLength: ").append(mLength);
    sb.append(", mLocations: ").append(mLocations);
    sb.append(", mWorkerDirs: ").append(mWorkerDirs).append(")");
    return sb.toString();
  }
}
