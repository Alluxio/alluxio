/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.block;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link BlockHeartbeatReporter}.
 */
public class BlockHeartbeatReporterTest {
  private static final int SESSION_ID = 1;
  private static final BlockStoreLocation MEM_LOC = new BlockStoreLocation("MEM", 0);
  private static final BlockStoreLocation SSD_LOC = new BlockStoreLocation("SSD", 0);
  private static final BlockStoreLocation HDD_LOC = new BlockStoreLocation("HDD", 0);
  BlockHeartbeatReporter mReporter;

  /**
   * Sets up all dependencies before a test runs.
   */
  @Before
  public final void before() {
    mReporter = new BlockHeartbeatReporter();
  }

  private void moveBlock(long blockId, BlockStoreLocation newLocation) {
    BlockStoreLocation unusedOldLocation = new BlockStoreLocation("MEM", 0);
    mReporter.onMoveBlockByWorker(SESSION_ID, blockId, unusedOldLocation, newLocation);
  }

  private void removeBlock(long blockId) {
    mReporter.onRemoveBlockByWorker(SESSION_ID, blockId);
  }

  /**
   * Tests the {@link BlockHeartbeatReporter#generateReport()} method for an empty report.
   */
  @Test
  public void generateReportEmptyTest() {
    BlockHeartbeatReport report = mReporter.generateReport();
    Assert.assertTrue(report.getAddedBlocks().isEmpty());
    Assert.assertTrue(report.getRemovedBlocks().isEmpty());
  }

  /**
   * Tests the {@link BlockHeartbeatReporter#generateReport()} method to correctly generate a report
   * after moving block.
   */
  @Test
  public void generateReportMoveTest() {
    Long block1 = 1L;
    Long block2 = 2L;
    Long block3 = 3L;
    moveBlock(block1, MEM_LOC);
    moveBlock(block2, SSD_LOC);
    moveBlock(block3, HDD_LOC);
    BlockHeartbeatReport report = mReporter.generateReport();
    Map<String, List<Long>> addedBlocks = report.getAddedBlocks();

    // Block1 moved to memory
    List<Long> addedBlocksMem = addedBlocks.get("MEM");
    Assert.assertEquals(1, addedBlocksMem.size());
    Assert.assertEquals(block1, addedBlocksMem.get(0));

    // Block2 moved to ssd
    List<Long> addedBlocksSsd = addedBlocks.get("SSD");
    Assert.assertEquals(1, addedBlocksSsd.size());
    Assert.assertEquals(block2, addedBlocksSsd.get(0));

    // Block3 moved to hdd
    List<Long> addedBlocksHdd = addedBlocks.get("HDD");
    Assert.assertEquals(1, addedBlocksHdd.size());
    Assert.assertEquals(block3, addedBlocksHdd.get(0));
  }

  /**
   * Tests the {@link BlockHeartbeatReporter#generateReport()} method that generating a report
   * clears the state of the reporter.
   */
  @Test
  public void generateReportStateClearTest() {
    Long block1 = 1L;
    moveBlock(block1, MEM_LOC);

    // First report should have updates
    BlockHeartbeatReport report = mReporter.generateReport();
    Assert.assertFalse(report.getAddedBlocks().isEmpty());

    // Second report should not have updates
    BlockHeartbeatReport nextReport = mReporter.generateReport();
    Assert.assertTrue(nextReport.getAddedBlocks().isEmpty());
    Assert.assertTrue(nextReport.getRemovedBlocks().isEmpty());
  }

  /**
   * Tests the {@link BlockHeartbeatReporter#generateReport()} method to correctly generate a report
   * after removing blocks.
   */
  @Test
  public void generateReportRemoveTest() {
    Long block1 = 1L;
    Long block2 = 2L;
    Long block3 = 3L;
    removeBlock(block1);
    removeBlock(block2);
    removeBlock(block3);
    BlockHeartbeatReport report = mReporter.generateReport();

    // All blocks should be removed
    List<Long> removedBlocks = report.getRemovedBlocks();
    Assert.assertEquals(3, removedBlocks.size());
    Assert.assertTrue(removedBlocks.contains(block1));
    Assert.assertTrue(removedBlocks.contains(block2));
    Assert.assertTrue(removedBlocks.contains(block3));

    // No blocks should have been added
    Map<String, List<Long>> addedBlocks = report.getAddedBlocks();
    Assert.assertTrue(addedBlocks.isEmpty());
  }

  /**
   * Tests the {@link BlockHeartbeatReporter#generateReport()} method to correctly generate a report
   * after moving a block and the removing it.
   */
  @Test
  public void generateReportMoveThenRemoveTest() {
    Long block1 = 1L;
    moveBlock(block1, MEM_LOC);
    removeBlock(block1);

    // The block should not be in the added blocks list
    BlockHeartbeatReport report = mReporter.generateReport();
    Assert.assertEquals(null, report.getAddedBlocks().get("MEM"));

    // The block should be in the removed blocks list
    List<Long> removedBlocks = report.getRemovedBlocks();
    Assert.assertEquals(1, removedBlocks.size());
    Assert.assertTrue(removedBlocks.contains(block1));
  }
}
