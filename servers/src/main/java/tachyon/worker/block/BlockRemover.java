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

import java.io.IOException;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.exception.InvalidStateException;
import tachyon.exception.NotFoundException;

/**
 * Thread to remove block from master
 */
public class BlockRemover implements Callable<Boolean> {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private BlockStore mBlockStore;
  private long mSessionId;
  private long mBlockId;
  private BlockStoreLocation mLocation;

  public BlockRemover(BlockStore blockStore, long sessionId, long blockId,
      BlockStoreLocation location) {
    mBlockStore = blockStore;
    mSessionId = sessionId;
    mBlockId = blockId;
    mLocation = location;
  }

  @Override
  public Boolean call() {
    try {
      if (null == mLocation) {
        mBlockStore.removeBlock(mSessionId, mBlockId);
      } else {
        mBlockStore.removeBlock(mSessionId, mBlockId, mLocation);
      }
      return true;
    } catch (IOException ioe) {
      LOG.warn("Failed to remove block " + mBlockId + " due to concurrent read.");
    } catch (InvalidStateException e) {
      LOG.warn("Failed to remove block " + mBlockId + " due to block uncommitted.");
    } catch (NotFoundException e) {
      LOG.warn("Failed to remove block " + mBlockId + " due to block not found.");
    }
    return false;
  }
}
