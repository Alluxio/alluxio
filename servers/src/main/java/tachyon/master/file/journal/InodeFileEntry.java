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

package tachyon.master.file.journal;

import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;

import tachyon.master.block.BlockId;
import tachyon.master.file.meta.InodeFile;
import tachyon.master.journal.JournalEntryType;

public class InodeFileEntry extends InodeEntry {
  private final long mBlockSizeBytes;
  private final long mLength;
  private final boolean mCompleted;
  private final boolean mCacheable;
  private final List<Long> mBlocks;

  public InodeFileEntry(long creationTimeMs, long id, String name, long parentId,
      boolean persisted, boolean pinned, long lastModificationTimeMs, long blockSizeBytes,
      long length, boolean completed, boolean cacheable, List<Long> blocks) {
    super(creationTimeMs, id, name, parentId, persisted, pinned, lastModificationTimeMs);
    mBlockSizeBytes = blockSizeBytes;
    mLength = length;
    mCompleted = completed;
    mCacheable = cacheable;
    mBlocks = Preconditions.checkNotNull(blocks);
  }

  public InodeFile toInodeFile() {
    InodeFile inode =
        new InodeFile(mName, BlockId.getContainerId(mId), mParentId, mBlockSizeBytes,
            mCreationTimeMs);

    // Set flags.
    if (mCompleted) {
      inode.setCompleted(mLength);
    }
    if (mBlocks != null) {
      inode.setBlockIds(mBlocks);
    }
    inode.setPersisted(mPersisted);
    inode.setPinned(mPinned);
    inode.setCacheable(mCacheable);
    inode.setLastModificationTimeMs(mLastModificationTimeMs);

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
    parameters.put("completed", mCompleted);
    parameters.put("cacheable", mCacheable);
    parameters.put("blocks", mBlocks);
    return parameters;
  }
}
