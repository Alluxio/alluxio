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

import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link BlockHeartbeatReporter}.
 */
public final class BlockHeartbeatReporterTest {
  private static final int SESSION_ID = 1;
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
    mReporter = new BlockHeartbeatReporter();
  }

  private void moveBlock(long blockId, BlockStoreLocation newLocation) {
    BlockStoreLocation unusedOldLocation =
        new BlockStoreLocation(Constants.MEDIUM_MEM, 0, Constants.MEDIUM_MEM);
    mReporter.onMoveBlockByWorker(SESSION_ID, blockId, unusedOldLocation, newLocation);
  }

  private void removeBlock(long blockId) {
    mReporter.onRemoveBlockByWorker(SESSION_ID, blockId);
  }

  /**
   * Tests the {@link BlockHeartbeatReporter#generateReport()} method for an empty report.
   */
  @Test
  public void generateReportEmpty() {
    BlockHeartbeatReport report = mReporter.generateReport();
    assertTrue(report.getAddedBlocks().isEmpty());
    assertTrue(report.getRemovedBlocks().isEmpty());
  }

  /**
   * Tests the {@link BlockHeartbeatReporter#generateReport()} method to correctly generate a report
   * after moving block.
   */
  @Test
  public void generateReportMove() {
    Long block1 = 1L;
    Long block2 = 2L;
    Long block3 = 3L;
    moveBlock(block1, MEM_LOC);
    moveBlock(block2, SSD_LOC);
    moveBlock(block3, HDD_LOC);
    BlockHeartbeatReport report = mReporter.generateReport();
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
   * Tests the {@link BlockHeartbeatReporter#generateReport()} method that generating a report
   * clears the state of the reporter.
   */
  @Test
  public void generateReportStateClear() {
    Long block1 = 1L;
    moveBlock(block1, MEM_LOC);

    // First report should have updates
    BlockHeartbeatReport report = mReporter.generateReport();
    assertFalse(report.getAddedBlocks().isEmpty());

    // Second report should not have updates
    BlockHeartbeatReport nextReport = mReporter.generateReport();
    assertTrue(nextReport.getAddedBlocks().isEmpty());
    assertTrue(nextReport.getRemovedBlocks().isEmpty());
  }

  /**
   * Tests the {@link BlockHeartbeatReporter#generateReport()} method to correctly generate a report
   * after removing blocks.
   */
  @Test
  public void generateReportRemove() {
    Long block1 = 1L;
    Long block2 = 2L;
    Long block3 = 3L;
    removeBlock(block1);
    removeBlock(block2);
    removeBlock(block3);
    BlockHeartbeatReport report = mReporter.generateReport();

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
   * Tests the {@link BlockHeartbeatReporter#generateReport()} method to correctly generate a report
   * after moving a block and the removing it.
   */
  @Test
  public void generateReportMoveThenRemove() {
    Long block1 = 1L;
    moveBlock(block1, MEM_LOC);
    removeBlock(block1);

    // The block should not be in the added blocks list
    BlockHeartbeatReport report = mReporter.generateReport();
    assertEquals(null, report.getAddedBlocks().get(MEM_LOC));

    // The block should be in the removed blocks list
    List<Long> removedBlocks = report.getRemovedBlocks();
    assertEquals(1, removedBlocks.size());
    assertTrue(removedBlocks.contains(block1));
  }
}
