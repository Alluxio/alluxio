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

import alluxio.client.file.cache.PageId;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.curator.shaded.com.google.common.collect.Sets;
import org.junit.Test;

import java.util.List;
import java.util.Objects;
import java.util.Set;

public class BlockPageIdTest {
  @Test
  public void equality() {
    BlockPageId id1 = new BlockPageId(1, 1);
    BlockPageId id2 = new BlockPageId("1", 1);
    BlockPageId id3 = new BlockPageId(1, 1);
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
    BlockPageId id1 = new BlockPageId("1", 1);
    PageId id2 = new PageId("1", 1);
    assertEquals(id1, id2);
    assertEquals(id2, id1);
  }

  @Test
  public void inequalityWithOtherSubclass() {
    PageId id = new BlockPageId("1", 1);
    PageId otherSubclassId = new MoreFieldsPageId("1", 1, 1);
    assertNotEquals(id, otherSubclassId);
    assertNotEquals(otherSubclassId, id);
  }

  @Test
  public void inequality() {
    Set<Object> pageIds = ImmutableSet.of(
        new BlockPageId(1, 1),
        new BlockPageId(2, 1),
        new BlockPageId(1, 2),
        new BlockPageId(2, 2),
        new Object());

    for (Set<Object> set : Sets.combinations(pageIds, 2)) {
      List<Object> pair = ImmutableList.copyOf(set);
      assertNotEquals(pair.get(0), pair.get(1));
      assertNotEquals(pair.get(1), pair.get(0));
    }
  }

  @Test
  public void testHashCode() {
    PageId id1 = new BlockPageId(1, 1);
    PageId id2 = new BlockPageId("1", 1);
    assertEquals(id1, id2);
    assertEquals(id1.hashCode(), id2.hashCode());

    PageId id3 = new PageId("1", 1);
    assertEquals(id1, id3);
    assertEquals(id1.hashCode(), id3.hashCode());
  }

  @Test
  public void getBlockId() {
    long blockId = 2;
    BlockPageId pageId = new BlockPageId(blockId, 0);
    assertEquals(blockId, pageId.getBlockId());
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
