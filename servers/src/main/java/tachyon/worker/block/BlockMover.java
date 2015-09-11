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

import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;

public class BlockMover implements Callable<Boolean> {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private final long mUserId;
  private final BlockStore mBlockStore;
  private final long mBlockId;
  private BlockStoreLocation mSrcLocation;
  private BlockStoreLocation mDstLocation;

  public BlockMover(BlockStore blockStore, long userId, long blockId,
      BlockStoreLocation srcLocation, BlockStoreLocation dstLocation) {
    mUserId = userId;
    mBlockStore = blockStore;
    mBlockId = blockId;
    mSrcLocation = srcLocation;
    mDstLocation = dstLocation;
  }

  public Boolean call() {
    String errorInfo = null;
    try {
      if (null == mDstLocation) {
        mBlockStore.removeBlock(mUserId, mBlockId, mSrcLocation);
        errorInfo = "Failed to remove block " + mBlockId + " at location: " + mSrcLocation;
      } else {
        mBlockStore.moveBlock(mUserId, mBlockId, mSrcLocation, mDstLocation);
        errorInfo = "Failed to move block" + mBlockId + " from " + mSrcLocation + " to "
            + mDstLocation;
      }
      return true;
    } catch (Exception e) {
      LOG.error(errorInfo, e);
      return false;
    }
  }
}

