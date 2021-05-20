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

package alluxio.master.block.meta;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.Constants;
import alluxio.MasterStorageTierAssoc;
import alluxio.StorageTierAssoc;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Unit tests for {@link MasterWorkerInfo}.
 */
public final class MasterWorkerInfoTest {
  private static final List<String> STORAGE_TIER_ALIASES =
      Lists.newArrayList(Constants.MEDIUM_MEM, Constants.MEDIUM_SSD);
  private static final StorageTierAssoc GLOBAL_STORAGE_TIER_ASSOC = new MasterStorageTierAssoc(
      STORAGE_TIER_ALIASES);
  private static final Map<String, Long> TOTAL_BYTES_ON_TIERS =
      ImmutableMap.of(Constants.MEDIUM_MEM, Constants.KB * 3L,
          Constants.MEDIUM_SSD, Constants.KB * 3L);
  private static final Map<String, Long> USED_BYTES_ON_TIERS =
      ImmutableMap.of(Constants.MEDIUM_MEM, (long) Constants.KB,
          Constants.MEDIUM_SSD, (long) Constants.KB);
  private static final Set<Long> NEW_BLOCKS = Sets.newHashSet(1L, 2L);
  private MasterWorkerInfo mInfo;

  /** The exception exptected to be thrown. */
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  /**
   * Sets up a new {@link MasterWorkerInfo} before a test runs.
   */
  @Before
  public void before() {
    // register
    mInfo = new MasterWorkerInfo(0, new WorkerNetAddress());
    mInfo.register(GLOBAL_STORAGE_TIER_ASSOC, STORAGE_TIER_ALIASES, TOTAL_BYTES_ON_TIERS,
        USED_BYTES_ON_TIERS, NEW_BLOCKS);
  }

  /**
   * Tests the {@link MasterWorkerInfo#register(StorageTierAssoc, List, Map, Map, Set)} method.
   */
  @Test
  public void register() {
    assertEquals(NEW_BLOCKS, mInfo.getBlocks());
    assertEquals(TOTAL_BYTES_ON_TIERS, mInfo.getTotalBytesOnTiers());
    assertEquals(Constants.KB * 6L, mInfo.getCapacityBytes());
    assertEquals(USED_BYTES_ON_TIERS, mInfo.getUsedBytesOnTiers());
    assertEquals(Constants.KB * 2L, mInfo.getUsedBytes());
  }

  /**
   * Tests the {@link MasterWorkerInfo#getFreeBytesOnTiers()} method.
   */
  @Test
  public void getFreeBytesOnTiers() {
    assertEquals(ImmutableMap.of(Constants.MEDIUM_MEM, Constants.KB * 2L,
        Constants.MEDIUM_SSD, Constants.KB * 2L),
        mInfo.getFreeBytesOnTiers());
  }

  /**
   * Tests that re-registering via
   * {@link MasterWorkerInfo#register(StorageTierAssoc, List, Map, Map, Set)} works.
   */
  @Test
  public void registerAgain() {
    Set<Long> newBlocks = Sets.newHashSet(3L);
    Set<Long> removedBlocks = mInfo.register(GLOBAL_STORAGE_TIER_ASSOC, STORAGE_TIER_ALIASES,
        TOTAL_BYTES_ON_TIERS, USED_BYTES_ON_TIERS, newBlocks);
    assertEquals(NEW_BLOCKS, removedBlocks);
    assertEquals(newBlocks, mInfo.getBlocks());
  }

  /**
   * Tests that an exception is thrown when trying to use the
   * {@link MasterWorkerInfo#register(StorageTierAssoc, List, Map, Map, Set)} method with a
   * different number of tiers.
   */
  @Test
  public void registerWithDifferentNumberOfTiers() {
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage("totalBytesOnTiers and usedBytesOnTiers should have the same number of"
        + " tiers as storageTierAliases, but storageTierAliases has 2 tiers, while"
        + " totalBytesOnTiers has 2 tiers and usedBytesOnTiers has 1 tiers");

    mInfo.register(GLOBAL_STORAGE_TIER_ASSOC, STORAGE_TIER_ALIASES, TOTAL_BYTES_ON_TIERS,
        ImmutableMap.of(Constants.MEDIUM_SSD, (long) Constants.KB), NEW_BLOCKS);
  }

  /**
   * Tests the {@link MasterWorkerInfo#getBlocks()} method.
   */
  @Test
  public void blockOperation() {
    // add existing block
    mInfo.addBlock(1L);
    assertEquals(NEW_BLOCKS, mInfo.getBlocks());
    // add a new block
    mInfo.addBlock(3L);
    assertTrue(mInfo.getBlocks().contains(3L));
    // remove block
    mInfo.removeBlock(3L);
    assertFalse(mInfo.getBlocks().contains(3L));
  }

  /**
   * Tests the {@link MasterWorkerInfo#generateWorkerInfo} method.
   */
  @Test
  public void workerInfoGeneration() {
    WorkerInfo workerInfo = mInfo.generateWorkerInfo(null, true);
    assertEquals(mInfo.getId(), workerInfo.getId());
    assertEquals(mInfo.getWorkerAddress(), workerInfo.getAddress());
    assertEquals("In Service", workerInfo.getState());
    assertEquals(mInfo.getCapacityBytes(), workerInfo.getCapacityBytes());
    assertEquals(mInfo.getUsedBytes(), workerInfo.getUsedBytes());
    assertEquals(mInfo.getStartTime(), workerInfo.getStartTimeMs());
  }

  /**
   * Tests the {@link MasterWorkerInfo#updateToRemovedBlock(boolean, long)} method.
   */
  @Test
  public void updateToRemovedBlock() {
    // remove a non-existing block
    mInfo.updateToRemovedBlock(true, 10L);
    assertTrue(mInfo.getToRemoveBlocks().isEmpty());
    // remove block 1
    mInfo.updateToRemovedBlock(true, 1L);
    assertTrue(mInfo.getToRemoveBlocks().contains(1L));
    // cancel the removal
    mInfo.updateToRemovedBlock(false, 1L);
    assertTrue(mInfo.getToRemoveBlocks().isEmpty());
    // actually remove 1 for real
    mInfo.updateToRemovedBlock(true, 1L);
    mInfo.removeBlock(1L);
    assertTrue(mInfo.getToRemoveBlocks().isEmpty());
  }

  /**
   * Tests the {@link MasterWorkerInfo#updateUsedBytes(Map)} method.
   */
  @Test
  public void updateUsedBytes() {
    assertEquals(Constants.KB * 2L, mInfo.getUsedBytes());
    Map<String, Long> usedBytesOnTiers =
        ImmutableMap.of(Constants.MEDIUM_MEM, Constants.KB * 2L,
            Constants.MEDIUM_SSD, (long) Constants.KB);
    mInfo.updateUsedBytes(usedBytesOnTiers);
    assertEquals(usedBytesOnTiers, mInfo.getUsedBytesOnTiers());
    assertEquals(Constants.KB * 3L, mInfo.getUsedBytes());
  }

  /**
   * Tests the {@link MasterWorkerInfo#updateUsedBytes(String, long)} method.
   */
  @Test
  public void updateUsedBytesInTier() {
    assertEquals(Constants.KB * 2L, mInfo.getUsedBytes());
    mInfo.updateUsedBytes(Constants.MEDIUM_MEM, Constants.KB * 2L);
    assertEquals(Constants.KB * 3L, mInfo.getUsedBytes());
    assertEquals(Constants.KB * 2L, (long) mInfo.getUsedBytesOnTiers().get(Constants.MEDIUM_MEM));
  }
}
