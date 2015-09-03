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

package tachyon.master.filesystem.journal;

import java.util.List;
import java.util.Map;

import tachyon.master.block.BlockId;
import tachyon.master.filesystem.meta.InodeFile;
import tachyon.master.journal.JournalEntryType;

public class InodeFileEntry extends InodeEntry {
  private final long mBlockSizeBytes;
  private final long mLength;
  private final boolean mIsComplete;
  private final boolean mIsCache;
  private final String mUfsPath;
  private final List<Long> mBlocks;

  public InodeFileEntry(long creationTimeMs, long id, String name, long parentId, boolean isPinned,
      long lastModificationTimeMs, long blockSizeBytes, long length, boolean isComplete,
      boolean isCache, String ufsPath, List<Long> blocks) {
    super(creationTimeMs, id, name, parentId, isPinned, lastModificationTimeMs);
    mBlockSizeBytes = blockSizeBytes;
    mLength = length;
    mIsComplete = isComplete;
    mIsCache = isCache;
    mUfsPath = ufsPath;
    mBlocks = blocks;
  }

  public InodeFile toInodeFile() {
    InodeFile inode = new InodeFile(mName, BlockId.getContainerId(mId), mParentId, mBlockSizeBytes,
        mCreationTimeMs);

    // Set flags.
    if (mIsComplete) {
      inode.setComplete(mLength);
    }
    inode.setPinned(mIsPinned);
    inode.setCache(mIsCache);

    return inode;
  }

  @Override
  public JournalEntryType getType() {
    return JournalEntryType.INODE_FILE;
  }

  @Override
  public Map<String, Object> getParameters() {
    Map<String, Object> parameters = super.getParameters();
    parameters.put("blockSizeBytes", mBlockSizeBytes);
    parameters.put("length", mLength);
    parameters.put("isComplete", mIsComplete);
    parameters.put("isCache", mIsCache);
    parameters.put("ufsPath", mUfsPath);
    parameters.put("blocks", mBlocks);
    return parameters;
  }
}
