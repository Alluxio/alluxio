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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
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
  private final long mTTL;

  @JsonCreator
  public InodeFileEntry(@JsonProperty("creationTimeMs") long creationTimeMs,
      @JsonProperty("id") long id, @JsonProperty("name") String name,
      @JsonProperty("parentId") long parentId, @JsonProperty("persisted") boolean persisted,
      @JsonProperty("pinned") boolean pinned,
      @JsonProperty("lastModificationTimeMs") long lastModificationTimeMs,
      @JsonProperty("blockSizeBytes") long blockSizeBytes, @JsonProperty("length") long length,
      @JsonProperty("completed") boolean completed, @JsonProperty("cacheable") boolean cacheable,
      @JsonProperty("blocks") List<Long> blocks, @JsonProperty("ttl") long ttl) {
    super(creationTimeMs, id, name, parentId, persisted, pinned, lastModificationTimeMs);
    mBlockSizeBytes = blockSizeBytes;
    mLength = length;
    mCompleted = completed;
    mCacheable = cacheable;
    mBlocks = Preconditions.checkNotNull(blocks);
    mTTL = ttl;
  }

  public InodeFile toInodeFile() {
    InodeFile inode =
        new InodeFile.Builder().setName(mName).setBlockContainerId(BlockId.getContainerId(mId))
            .setParentId(mParentId).setBlockSizeBytes(mBlockSizeBytes)
            .setCreationTimeMs(mCreationTimeMs).setTTL(mTTL).setPersisted(mPersisted).build();

    if (mBlocks != null) {
      inode.setBlockIds(mBlocks);
    }
    inode.setCompleted(mCompleted);
    inode.setLength(mLength);
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

  @JsonGetter
  public long getBlockSizeBytes() {
    return mBlockSizeBytes;
  }

  @JsonGetter
  public long getLength() {
    return mLength;
  }

  @JsonGetter
  public boolean isCompleted() {
    return mCompleted;
  }

  @JsonGetter
  public boolean isCacheable() {
    return mCacheable;
  }

  @JsonGetter
  public List<Long> getBlocks() {
    return mBlocks;
  }

  @JsonGetter
  public long getTtl() {
    return mTTL;
  }
}
