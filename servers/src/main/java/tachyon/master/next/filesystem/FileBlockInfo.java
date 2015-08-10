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

package tachyon.master.next.filesystem;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;

import tachyon.StorageDirId;
import tachyon.StorageLevelAlias;
import tachyon.conf.TachyonConf;
import tachyon.master.next.block.meta.BlockId;
import tachyon.thrift.NetAddress;
import tachyon.underfs.UnderFileSystem;

/**
 * Block info on the master side.
 */
public class FileBlockInfo {
  private final InodeFile mInodeFile;

  public final int mBlockIndex;
  public final long mBlockId;
  public final long mOffset;
  public final long mLength;

  /**
   * @param inodeFile
   * @param blockIndex
   * @param length must be smaller than 2^31 (i.e., 2GB)
   */
  public FileBlockInfo(InodeFile inodeFile, int blockIndex, long length) {
    Preconditions.checkArgument((length >> 31) == 0, "length must be smaller than 2^31");
    mInodeFile = inodeFile;
    mBlockIndex = blockIndex;
    mBlockId = BlockId.createBlockId(mInodeFile.getBlockContainerId(), mBlockIndex);
    mOffset = inodeFile.getBlockSizeByte() * blockIndex;
    mLength = length;
  }

  /**
   * Get the locations of the block, which are the workers' net address who has the data of the
   * block in its tiered storage. The list is sorted by the storage level alias(MEM, SSD, HDD).
   * That is, the worker who has the data of the block in its memory is in the top of the list.
   *
   * @return the net addresses of the locations
   */
  public synchronized List<NetAddress> getLocations(TachyonConf tachyonConf) {
    List<NetAddress> ret = new ArrayList<NetAddress>();
    if (ret.isEmpty() && mInodeFile.hasCheckpointed()) {
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
              resolvedHost = ipport[0];
              resolvedPort = Integer.parseInt(ipport[1]);
            }
          } catch (NumberFormatException nfe) {
            continue;
          }
          // The resolved port is the data transfer port not the rpc port
          ret.add(new NetAddress(resolvedHost, -1, resolvedPort));
        }
      }
    }
    return ret;
  }

  /**
   * @return true if the block is in some worker's memory, false otherwise
   */
  public synchronized boolean isInMemory() {
    // TODO
    return false;
  }

  @Override
  public synchronized String toString() {
    StringBuilder sb = new StringBuilder("BlockInfo(mBlockIndex: ");
    sb.append(mBlockIndex);
    sb.append(", mBlockId: ").append(mBlockId);
    sb.append(", mOffset: ").append(mOffset);
    sb.append(", mLength: ").append(mLength);
    return sb.toString();
  }
}
