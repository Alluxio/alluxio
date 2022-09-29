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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;

import alluxio.client.file.cache.PageId;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.curator.shaded.com.google.common.collect.Sets;
import org.junit.Test;

import java.util.List;
import java.util.Objects;
import java.util.Set;

public class BlockPageIdTest {
  private static final long BLOCK_SIZE = 0;
  @Test
  public void equality() {
    BlockPageId id1 = new BlockPageId(1, 1, BLOCK_SIZE);
    BlockPageId id2 = new BlockPageId("1", 1, BLOCK_SIZE);
    BlockPageId id3 = new BlockPageId(1, 1, BLOCK_SIZE);
    // reflexive
    assertEquals(id1, id1);
    assertEquals(id2, id2);
    assertEquals(id3, id3);
    // symmetric
    assertEquals(id1, id2);
    assertEquals(id2, id1);
    // transitive
    assertEquals(id1, id2);
    assertEquals(id2, id3);
    assertEquals(id3, id1);
  }

  @Test
  public void equalityWithParentClass() {
    BlockPageId id1 = new BlockPageId("1", 1, BLOCK_SIZE);
    PageId id2 = new PageId(BlockPageId.fileIdOf(1, BLOCK_SIZE), 1);
    assertEquals(id1, id2);
    assertEquals(id2, id1);
  }

  @Test
  public void inequalityWithOtherSubclass() {
    PageId id = new BlockPageId("1", 1, BLOCK_SIZE);
    PageId otherSubclassId = new MoreFieldsPageId("1", 1, BLOCK_SIZE);
    assertNotEquals(id, otherSubclassId);
    assertNotEquals(otherSubclassId, id);
  }

  @Test
  public void inequality() {
    Set<Object> pageIds = ImmutableSet.of(
        new BlockPageId(1, 1, BLOCK_SIZE),
        new BlockPageId(2, 1, BLOCK_SIZE),
        new BlockPageId(1, 2, BLOCK_SIZE),
        new BlockPageId(2, 2, BLOCK_SIZE),
        new Object());

    for (Set<Object> set : Sets.combinations(pageIds, 2)) {
      List<Object> pair = ImmutableList.copyOf(set);
      assertNotEquals(pair.get(0), pair.get(1));
      assertNotEquals(pair.get(1), pair.get(0));
    }
  }

  @Test
  public void testHashCode() {
    PageId id1 = new BlockPageId(1, 1, BLOCK_SIZE);
    PageId id2 = new BlockPageId("1", 1, BLOCK_SIZE);
    assertEquals(id1, id2);
    assertEquals(id1.hashCode(), id2.hashCode());

    PageId id3 = new PageId(BlockPageId.fileIdOf(1, BLOCK_SIZE), 1);
    assertEquals(id1, id3);
    assertEquals(id1.hashCode(), id3.hashCode());
  }

  @Test
  public void getBlockId() {
    long blockId = 2;
    BlockPageId pageId = new BlockPageId(blockId, 0, BLOCK_SIZE);
    assertEquals(blockId, pageId.getBlockId());
  }

  @Test
  public void getBlockSize() {
    long blockSize = 42;
    BlockPageId pageId = new BlockPageId(1, 0, blockSize);
    assertEquals(blockSize, pageId.getBlockSize());
  }

  @Test
  public void downcastOk() {
    PageId wellFormed = new PageId("paged_block_1234567890abcdef_size_0123cafebabedead", 0);
    BlockPageId downcast = BlockPageId.tryDowncast(wellFormed);
    assertEquals(wellFormed, downcast);
    assertEquals(0x1234_5678_90ab_cdefL, downcast.getBlockId());
    assertEquals(0x1234_5678_90ab_cdefL, downcast.getBlockId());
    assertEquals(0x0123_cafe_babe_deadL, downcast.getBlockSize());

    BlockPageId self = new BlockPageId(1234L, 0, BLOCK_SIZE);
    assertEquals(self, BlockPageId.tryDowncast(self));
  }

  @Test
  public void downcastWrong() {
    PageId noPrefix = new PageId("_1234567890abcdef_size_0123cafebabedead", 0);
    assertThrows(IllegalArgumentException.class, () -> BlockPageId.tryDowncast(noPrefix));
    PageId notEnoughDigits = new PageId("paged_block_size_1234_cafe", 0);
    assertThrows(IllegalArgumentException.class, () -> BlockPageId.tryDowncast(notEnoughDigits));
    PageId empty = new PageId("", 0);
    assertThrows(IllegalArgumentException.class, () -> BlockPageId.tryDowncast(empty));
    PageId extraSuffix = new PageId("paged_block_1234567890abcdef_size_0123cafebabedead.parquet", 0);
    assertThrows(IllegalArgumentException.class, () -> BlockPageId.tryDowncast(extraSuffix));
    PageId longOverflow = new PageId("paged_block_1234567890abcdef_size_cafebabedead0123", 0);
    assertThrows(IllegalArgumentException.class, () -> BlockPageId.tryDowncast(longOverflow));
  }

  private static class MoreFieldsPageId extends PageId {
    private final long mSomeField;

    public MoreFieldsPageId(String fileId, long pageIndex, long value) {
      super(fileId, pageIndex);
      mSomeField = value;
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), mSomeField);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }
      MoreFieldsPageId that = (MoreFieldsPageId) o;
      return mSomeField == that.mSomeField;
    }
  }
}
