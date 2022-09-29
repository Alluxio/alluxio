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
    BlockPageId id1 = new BlockPageId(1, 1, 0);
    BlockPageId id2 = new BlockPageId("1", 1, 0);
    BlockPageId id3 = new BlockPageId(1, 1, 0);
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
    BlockPageId id1 = new BlockPageId("1", 1, 0);
    PageId id2 = new PageId("1", 1);
    assertEquals(id1, id2);
    assertEquals(id2, id1);
  }

  @Test
  public void inequalityWithOtherSubclass() {
    PageId id = new BlockPageId("1", 1, 1);
    PageId otherSubclassId = new MoreFieldsPageId("1", 1, 1);
    assertNotEquals(id, otherSubclassId);
    assertNotEquals(otherSubclassId, id);
  }

  @Test
  public void inequality() {
    Set<Object> pageIds = ImmutableSet.of(
        new BlockPageId(1, 1, 0),
        new BlockPageId(2, 1, 0),
        new BlockPageId(1, 2, 0),
        new BlockPageId(2, 2, 0),
        new Object());

    for (Set<Object> set : Sets.combinations(pageIds, 2)) {
      List<Object> pair = ImmutableList.copyOf(set);
      assertNotEquals(pair.get(0), pair.get(1));
      assertNotEquals(pair.get(1), pair.get(0));
    }
  }

  @Test
  public void testHashCode() {
    PageId id1 = new BlockPageId(1, 1, 0);
    PageId id2 = new BlockPageId("1", 1, 0);
    assertEquals(id1, id2);
    assertEquals(id1.hashCode(), id2.hashCode());

    PageId id3 = new PageId("1", 1);
    assertEquals(id1, id3);
    assertEquals(id1.hashCode(), id3.hashCode());
  }

  @Test
  public void getBlockId() {
    long blockId = 2;
    BlockPageId pageId = new BlockPageId(blockId, 0, 0);
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
    PageId wellFormed = new PageId("paged_block_123456789abcdef_cafebeefbabedead", 0);
    BlockPageId downcast = BlockPageId.tryDowncast(wellFormed);
    assertEquals(0x123456789abcdefL, downcast.getBlockId());
    assertEquals(0x123456789abcdefL, downcast.getBlockId());
    assertEquals(0xcafebeefbabedeadL, downcast.getBlockSize());

    BlockPageId self = new BlockPageId(1234L, 0, 0);
    assertEquals(self, BlockPageId.tryDowncast(self));
  }

  @Test
  public void downcastWrong() {
    PageId noPrefix = new PageId("_123456789abcdef_cafebeefbabedead", 0);
    assertThrows(IllegalArgumentException.class, () -> BlockPageId.tryDowncast(noPrefix));
    PageId notEnoughDigits = new PageId("paged_block_1234_cafe", 0);
    assertThrows(IllegalArgumentException.class, () -> BlockPageId.tryDowncast(notEnoughDigits));
    PageId empty = new PageId("", 0);
    assertThrows(IllegalArgumentException.class, () -> BlockPageId.tryDowncast(empty));
    PageId extraSuffix = new PageId("paged_block_123456789abcdef_cafebeefbabedead.parquet", 0);
    assertThrows(IllegalArgumentException.class, () -> BlockPageId.tryDowncast(extraSuffix));
  }

  private static class MoreFieldsPageId extends PageId {
    private final int mSomeField;

    public MoreFieldsPageId(String fileId, long pageIndex, int value) {
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
