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

package alluxio.worker.block;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.Constants;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;

import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link BlockHeartbeatReporter}.
 */
public final class BlockHeartbeatReporterTest {
  private static final BlockStoreLocation MEM_LOC =
      new BlockStoreLocation(Constants.MEDIUM_MEM, 0, Constants.MEDIUM_MEM);
  private static final BlockStoreLocation SSD_LOC =
      new BlockStoreLocation(Constants.MEDIUM_SSD, 0, Constants.MEDIUM_SSD);
  private static final BlockStoreLocation HDD_LOC =
      new BlockStoreLocation(Constants.MEDIUM_HDD, 0, Constants.MEDIUM_HDD);
  BlockHeartbeatReporter mReporter;

  /**
   * Sets up all dependencies before a test runs.
   */
  @Before
  public final void before() {
    Configuration.set(PropertyKey.WORKER_REGISTER_TO_ALL_MASTERS, true);
    mReporter = new BlockHeartbeatReporter();
  }

  private void moveBlock(long blockId, BlockStoreLocation newLocation) {
    BlockStoreLocation unusedOldLocation =
        new BlockStoreLocation(Constants.MEDIUM_MEM, 0, Constants.MEDIUM_MEM);
    mReporter.onMoveBlockByWorker(blockId, unusedOldLocation, newLocation);
  }

  private void removeBlock(long blockId) {
    mReporter.onRemoveBlockByWorker(blockId);
  }

  /**
   * Tests the {@link BlockHeartbeatReporter#generateReportAndClear()} method for an empty report.
   */
  @Test
  public void generateReportEmpty() {
    BlockHeartbeatReport report = mReporter.generateReportAndClear();
    assertTrue(report.getAddedBlocks().isEmpty());
    assertTrue(report.getRemovedBlocks().isEmpty());
  }

  /**
   * Tests the {@link BlockHeartbeatReporter#generateReportAndClear()}
   * method to correctly generate a report after moving block.
   */
  @Test
  public void generateReportMove() {
    Long block1 = 1L;
    Long block2 = 2L;
    Long block3 = 3L;
    moveBlock(block1, MEM_LOC);
    moveBlock(block2, SSD_LOC);
    moveBlock(block3, HDD_LOC);
    BlockHeartbeatReport report = mReporter.generateReportAndClear();
    Map<BlockStoreLocation, List<Long>> addedBlocks = report.getAddedBlocks();

    // Block1 moved to memory
    List<Long> addedBlocksMem = addedBlocks.get(MEM_LOC);
    assertEquals(1, addedBlocksMem.size());
    assertEquals(block1, addedBlocksMem.get(0));

    // Block2 moved to ssd
    List<Long> addedBlocksSsd = addedBlocks.get(SSD_LOC);
    assertEquals(1, addedBlocksSsd.size());
    assertEquals(block2, addedBlocksSsd.get(0));

    // Block3 moved to hdd
    List<Long> addedBlocksHdd = addedBlocks.get(HDD_LOC);
    assertEquals(1, addedBlocksHdd.size());
    assertEquals(block3, addedBlocksHdd.get(0));
  }

  /**
   * Tests the {@link BlockHeartbeatReporter#generateReportAndClear()}
   * method that generating a report clears the state of the reporter.
   */
  @Test
  public void generateReportStateClear() {
    Long block1 = 1L;
    moveBlock(block1, MEM_LOC);

    // First report should have updates
    BlockHeartbeatReport report = mReporter.generateReportAndClear();
    assertFalse(report.getAddedBlocks().isEmpty());

    // Second report should not have updates
    BlockHeartbeatReport nextReport = mReporter.generateReportAndClear();
    assertTrue(nextReport.getAddedBlocks().isEmpty());
    assertTrue(nextReport.getRemovedBlocks().isEmpty());
  }

  /**
   * Tests the {@link BlockHeartbeatReporter#generateReportAndClear()}
   * method to correctly generate a report after removing blocks.
   */
  @Test
  public void generateReportRemove() {
    Long block1 = 1L;
    Long block2 = 2L;
    Long block3 = 3L;
    removeBlock(block1);
    removeBlock(block2);
    removeBlock(block3);
    BlockHeartbeatReport report = mReporter.generateReportAndClear();

    // All blocks should be removed
    List<Long> removedBlocks = report.getRemovedBlocks();
    assertEquals(3, removedBlocks.size());
    assertTrue(removedBlocks.contains(block1));
    assertTrue(removedBlocks.contains(block2));
    assertTrue(removedBlocks.contains(block3));

    // No blocks should have been added
    Map<BlockStoreLocation, List<Long>> addedBlocks = report.getAddedBlocks();
    assertTrue(addedBlocks.isEmpty());
  }

  /**
   * Tests the {@link BlockHeartbeatReporter#generateReportAndClear()}
   * method to correctly generate a report after moving a block and the removing it.
   */
  @Test
  public void generateReportMoveThenRemove() {
    Long block1 = 1L;
    moveBlock(block1, MEM_LOC);
    removeBlock(block1);

    // The block should not be in the added blocks list
    BlockHeartbeatReport report = mReporter.generateReportAndClear();
    assertEquals(null, report.getAddedBlocks().get(MEM_LOC));

    // The block should be in the removed blocks list
    List<Long> removedBlocks = report.getRemovedBlocks();
    assertEquals(1, removedBlocks.size());
    assertTrue(removedBlocks.contains(block1));
  }

  @Test
  public void generateAndRevert() {
    mReporter.onMoveBlockByWorker(1, MEM_LOC, SSD_LOC);
    mReporter.onMoveBlockByWorker(2, MEM_LOC, SSD_LOC);
    mReporter.onMoveBlockByWorker(3, SSD_LOC, HDD_LOC);
    mReporter.onRemoveBlockByClient(4);
    mReporter.onStorageLost(Constants.MEDIUM_MEM, "/foo");
    mReporter.onStorageLost(Constants.MEDIUM_MEM, "/bar");
    BlockHeartbeatReport originalReport = mReporter.generateReportAndClear();
    mReporter.mergeBack(originalReport);
    BlockHeartbeatReport newReport = mReporter.generateReportAndClear();
    assertEquals(originalReport.getAddedBlocks(), newReport.getAddedBlocks());
    assertEquals(originalReport.getRemovedBlocks(), newReport.getRemovedBlocks());
    assertEquals(originalReport.getLostStorage(), newReport.getLostStorage());
  }

  @Test
  public void generateUpdateThenRevert() {
    mReporter.onMoveBlockByWorker(1, HDD_LOC, MEM_LOC);
    mReporter.onMoveBlockByWorker(2, HDD_LOC, MEM_LOC);
    mReporter.onMoveBlockByWorker(3, HDD_LOC, SSD_LOC);
    mReporter.onRemoveBlockByClient(4);
    mReporter.onStorageLost(Constants.MEDIUM_MEM, "/foo");
    mReporter.onStorageLost(Constants.MEDIUM_HDD, "/bar");
    BlockHeartbeatReport originalReport = mReporter.generateReportAndClear();

    mReporter.onRemoveBlockByClient(1);
    mReporter.onRemoveBlockByClient(3);
    mReporter.onRemoveBlockByClient(5);
    mReporter.onMoveBlockByWorker(6, SSD_LOC, HDD_LOC);
    mReporter.onMoveBlockByWorker(7, HDD_LOC, MEM_LOC);
    mReporter.onStorageLost(Constants.MEDIUM_MEM, "/baz");
    mReporter.mergeBack(originalReport);
    BlockHeartbeatReport newReport = mReporter.generateReportAndClear();

    assertEquals(ImmutableMap.of(
        MEM_LOC, Arrays.asList(7L, 2L),
        HDD_LOC, Collections.singletonList(6L)
    ), newReport.getAddedBlocks());
    assertEquals(new HashSet<>(Arrays.asList(1L, 3L, 4L, 5L)),
        new HashSet<>(newReport.getRemovedBlocks()));
    assertEquals(2, newReport.getLostStorage().get(Constants.MEDIUM_MEM).size());
    assertEquals(1, newReport.getLostStorage().get(Constants.MEDIUM_HDD).size());
  }
}
