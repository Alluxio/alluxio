/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.page;

import alluxio.client.file.cache.PageId;

/**
 * Specialized {@link PageId} when it's part of a block.
 */
public final class BlockPageId extends PageId {
  /**
   * this is constructed from {@link PageId#getFileId()} and cached to avoid parsing the string
   * multiple times.
   */
  private final long mBlockId;

  /**
   * @param blockId the block ID
   * @param pageIndex index of the page in the block
   * @throws NumberFormatException when {@code blockId} cannot be parsed as a {@code long}
   */
  public BlockPageId(String blockId, long pageIndex) {
    super(blockId, pageIndex);
    mBlockId = Long.parseLong(blockId);
  }

  /**
   * Creates an instance with a block ID as a {@code long}.
   * @param blockId the block ID
   * @param pageIndex index of the page in the block
   */
  public BlockPageId(long blockId, long pageIndex) {
    super(String.valueOf(blockId).intern(), pageIndex);
    mBlockId = blockId;
  }

  /**
   * @return the block ID
   */
  public long getBlockId() {
    return mBlockId;
  }

  // CHECKSTYLE OFF: EqualsHashCode
  // hashCode and toString impls are intentionally not overridden
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PageId)) {
      return false;
    }
    // a fast path comparing longs instead of strings when both are BlockPageIds
    if (o instanceof BlockPageId) { // we are final so instanceof check is ok
      BlockPageId that = (BlockPageId) o;
      return mBlockId == that.mBlockId && getPageIndex() == that.getPageIndex();
    }
    // otherwise o is either the super class PageId or some other subclass of PageId.
    // super.equals(o) does not work here because if o is a subclass of PageId,
    // it may have its own unique fields, so need to call their equals method
    return o.equals(this);
  }
  // CHECKSTYLE ON: EqualsHashCode
}
